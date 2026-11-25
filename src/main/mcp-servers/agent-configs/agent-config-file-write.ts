// Publishing an edited agent config without blocking the thread: only while the file still holds
// the bytes the edit was made from, after copying those bytes to a rolling `.bak` beside it. The
// agent or an editor can save at any moment, so the file is taken aside before its last check and
// the edit goes in only where no newer save took the name: a save that lands meanwhile is kept.
// (atomic-file-publication's guarded write is synchronous and repairs folder ACLs, which suits
// Orca's own folders, not the user's home.)
import { createHash, randomUUID } from 'node:crypto'
import {
  chmod,
  constants,
  copyFile,
  link,
  lstat,
  readFile,
  readlink,
  realpath,
  rm,
  stat,
  writeFile
} from 'node:fs/promises'
import { dirname, resolve } from 'node:path'
import { renameFileWithWindowsRetryAsync } from '../../../shared/file-write/windows-retry-file-operations'
import { isDefinitiveAbsence } from '../../../shared/errors/definitive-filesystem-absence'
import { errnoCodeOrUnknown } from '../../../shared/errors/errno-code'
import { runKeyedSerializedOperation } from '../../../shared/concurrency/keyed-serialized-operations'
import { isHardLinkUnsupported } from '../../filesystem/hard-link-unsupported-error'
import { isReplaceableReadOnlyBackup } from '../../filesystem/rolling-file-backup'

async function lstatIfPresent(path: string) {
  try {
    return await lstat(path)
  } catch (error) {
    if (isDefinitiveAbsence(error)) {
      return null
    }
    throw error
  }
}

const MAX_LINK_HOPS = 8

/** The file `path` names after its dotfile symlinks, even while that file is held aside. */
async function publishedPath(path: string): Promise<string> {
  let current = path
  for (let hop = 0; hop < MAX_LINK_HOPS; hop++) {
    if (!(await lstatIfPresent(current))?.isSymbolicLink()) {
      return current
    }
    // A relative link is read from its folder's real place, which may itself be a link.
    current = resolve(await realpath(dirname(current)), await readlink(current))
  }
  throw Object.assign(new Error(`Too many symbolic links: ${path}`), { code: 'ELOOP' })
}

/** Where a config waits while its edit is published; only Orca writes this name. */
const heldPathOf = (target: string): string => `${target}.orca-publishing`

/** Moves `source` over the rolling `.bak`, never through a symlinked one. */
async function replaceBackup(source: string, backup: string): Promise<void> {
  const previous = await lstatIfPresent(backup)
  if (previous?.isSymbolicLink()) {
    throw new Error(`Refusing to overwrite symlinked backup: ${backup}`)
  }
  if (process.platform === 'win32') {
    // copyFile keeps a read-only attribute, and Windows refuses to rename over a read-only file.
    await chmod(source, 0o666)
    if (isReplaceableReadOnlyBackup(previous)) {
      await chmod(backup, 0o666)
    }
  }
  await renameFileWithWindowsRetryAsync(source, backup)
}

/** Gives `source` the name `target` unless a file already has it; false when one does. */
async function linkWithoutReplacing(source: string, target: string): Promise<boolean> {
  try {
    await link(source, target)
    return true
  } catch (error) {
    if (errnoCodeOrUnknown(error) === 'EEXIST') {
      return false
    }
    if (!isHardLinkUnsupported(error)) {
      throw error
    }
  }
  // FAT/exFAT and \\wsl$ have no hard links: an exclusive copy still never replaces a file.
  try {
    await copyFile(source, target, constants.COPYFILE_EXCL)
    return true
  } catch (error) {
    if (errnoCodeOrUnknown(error) === 'EEXIST') {
      return false
    }
    throw error
  }
}

/**
 * Puts a held config back. When a newer file has the name (a writer that found none), the held
 * bytes stay beside it as `<config>.orca-kept-<hash>`: they can be the user's only full copy.
 */
async function restoreHeld(held: string, target: string): Promise<void> {
  if (await linkWithoutReplacing(held, target)) {
    await rm(held, { force: true })
    return
  }
  const hash = createHash('sha256')
    .update(await readFile(held))
    .digest('hex')
    .slice(0, 16)
  // Named by content: a kept copy with this name already holds these bytes.
  await linkWithoutReplacing(held, `${target}.orca-kept-${hash}`)
  await rm(held, { force: true })
}

// One read or publish of a config at a time in this process, so none sees it while it's held.
const configQueues = new Map<string, Promise<void>>()
const oneAtATime = <T>(target: string, run: () => Promise<T>): Promise<T> =>
  runKeyedSerializedOperation(configQueues, target, run)

/** The file's text (BOM kept), or null when it is definitely absent. */
export async function readAgentConfigText(path: string): Promise<string | null> {
  const target = await publishedPath(path)
  return oneAtATime(target, async () => {
    // Orca quit while publishing an edit: the config goes back before anyone reads it.
    if (await lstatIfPresent(heldPathOf(target))) {
      await restoreHeld(heldPathOf(target), target)
    }
    try {
      return await readFile(path, 'utf8')
    } catch (error) {
      if (isDefinitiveAbsence(error)) {
        return null
      }
      throw error
    }
  })
}

/**
 * Runs `publish` with the config at `target` held aside, once its bytes are still `computedFrom`;
 * false when they aren't (put back) or when a newer file took the name meanwhile (it stays).
 */
function withConfigHeld(
  target: string,
  computedFrom: string,
  publish: () => Promise<boolean>
): Promise<boolean> {
  const held = heldPathOf(target)
  return oneAtATime(target, async () => {
    try {
      await renameFileWithWindowsRetryAsync(target, held)
    } catch (error) {
      if (isDefinitiveAbsence(error)) {
        return false
      }
      throw error
    }
    let published = false
    try {
      if ((await readFile(held)).equals(Buffer.from(computedFrom))) {
        published = await publish()
      }
      return published
    } finally {
      // Published (or deleted on purpose): the held bytes are the `.bak` already. Else they go back.
      await (published ? rm(held, { force: true }).catch(() => {}) : restoreHeld(held, target))
    }
  })
}

/**
 * Replaces `path` (a dotfile symlink's target) with `contents`; false, writing nothing, when the
 * file no longer holds `computedFrom`, so the caller redoes its edit on the newer save.
 */
export async function writeAgentConfigIfUnchanged(
  path: string,
  contents: string,
  computedFrom: string
): Promise<boolean> {
  const target = await publishedPath(path)
  const mode = (await stat(target)).mode & 0o777
  const temp = `${target}.${randomUUID()}.tmp`
  const backupTemp = `${target}.bak.${randomUUID()}.tmp`
  try {
    // Agent configs can hold tokens: the new file keeps the old one's permissions.
    await writeFile(temp, contents, { mode, flag: 'wx' })
    if (process.platform !== 'win32') {
      await chmod(temp, mode)
    }
    await copyFile(target, backupTemp)
    // A save that lands while the copy is made keeps the previous backup and refuses the edit.
    if ((await readAgentConfigText(target)) !== computedFrom) {
      return false
    }
    await replaceBackup(backupTemp, `${target}.bak`)
    return await withConfigHeld(target, computedFrom, () => linkWithoutReplacing(temp, target))
  } finally {
    // Best effort: a Windows scanner holding the published file's second name must not fail it.
    await Promise.all([temp, backupTemp].map((file) => rm(file, { force: true }).catch(() => {})))
  }
}

/** Deletes `path` only while it still holds `computedFrom`; a save that lands meanwhile stays. */
export async function removeAgentConfigIfUnchanged(
  path: string,
  computedFrom: string
): Promise<boolean> {
  const target = await publishedPath(path)
  if (target !== path) {
    // A dotfile link goes, never the file it points at (which keeps any save).
    if ((await readAgentConfigText(path)) !== computedFrom) {
      return false
    }
    await rm(path, { force: true })
    return true
  }
  return withConfigHeld(target, computedFrom, async () => true)
}
