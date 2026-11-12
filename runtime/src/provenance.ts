/** Release-approved identities, verified before constructing a native adapter. */
import {createHash} from 'node:crypto';
import {openSync, closeSync, readSync, fstatSync, lstatSync, readdirSync, readFileSync, constants} from 'node:fs';
import {dirname, join, relative, isAbsolute} from 'node:path';
import {fileURLToPath} from 'node:url';
import {execFile} from 'node:child_process';
import type {Provider} from './events.js';
import type {RuntimeBinding} from './runtime.js';
const packages = {codex: '@openai/codex-sdk', claude: '@anthropic-ai/claude-agent-sdk'};
export interface ApprovedRuntime {
  provider: Provider;
  sdk_version: string;
  sdk_sha256: string;
  cli_path: string;
  cli_version: string;
  cli_sha256: string;
}

export function fileDigest(path: string): string {
  const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const before = fstatSync(fd);
    if (!before.isFile()) throw new Error('runtime_not_regular_file');
    const digest = createHash('sha256'); const buffer = Buffer.alloc(64 * 1024);
    let count: number;
    while ((count = readSync(fd, buffer, 0, buffer.length, null)) !== 0) digest.update(buffer.subarray(0, count));
    const after = fstatSync(fd);
    if (before.size !== after.size || before.mtimeMs !== after.mtimeMs || before.ctimeMs !== after.ctimeMs) {
      throw new Error('runtime_changed_during_verification');
    }
    return digest.digest('hex');
  } finally { closeSync(fd); }
}

/** Canonical package-tree digest, including shipped CLI files; dependencies use npm lock integrity. */
export function packageDigest(root: string): string {
  const digest = createHash('sha256');
  const walk = (path: string) => {
    const info = lstatSync(path);
    if (info.isSymbolicLink()) throw new Error('runtime_symlink_refused');
    if (info.isDirectory()) {
      for (const name of readdirSync(path).sort()) {
        if (name === 'node_modules') continue;
        walk(join(path, name));
      }
    } else if (info.isFile()) {
      digest.update(JSON.stringify([relative(root, path).split('\\').join('/'), fileDigest(path)]) + '\n');
    } else throw new Error('runtime_not_regular_file');
  };
  walk(root); return digest.digest('hex');
}

export function installedPackage(provider: Provider): string {
  if (!(provider in packages)) throw new Error('unsupported_provider');
  let current = dirname(fileURLToPath(import.meta.resolve(packages[provider])));
  for (;;) {
    try {
      const info = JSON.parse(readFileSync(join(current, 'package.json'), 'utf8'));
      if (info.name === packages[provider]) return current;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
    }
    const parent = dirname(current);
    if (parent === current) throw new Error('sdk_package_not_found');
    current = parent;
  }
}

/** A trusted deployment must keep approved install paths immutable during execution. */
export async function verifyRuntime(approved: ApprovedRuntime): Promise<Readonly<RuntimeBinding>> {
  if (!Object.hasOwn(packages, approved.provider) || !isAbsolute(approved.cli_path) ||
      ![approved.sdk_sha256, approved.cli_sha256].every(s => /^[a-f0-9]{64}$/.test(s)) ||
      ![approved.sdk_version, approved.cli_version].every(s => /^\d+\.\d+\.\d+(?:-[a-zA-Z0-9.-]+)?$/.test(s))) {
    throw new Error('invalid_runtime_manifest');
  }
  const root = installedPackage(approved.provider);
  const info = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));
  if (info.version !== approved.sdk_version || packageDigest(root) !== approved.sdk_sha256 ||
      fileDigest(approved.cli_path) !== approved.cli_sha256) throw new Error('runtime_provenance_mismatch');
  const version = await new Promise<string>((resolve, reject) => {
    execFile(approved.cli_path, ['--version'], {timeout: 5000, maxBuffer: 4096,
      env: {PATH: '/usr/bin:/bin', LANG: 'C'}, windowsHide: true}, (error, stdout) => {
      if (error) reject(new Error('runtime_version_probe_failed')); else resolve(stdout.trim());
    });
  });
  const expected = approved.provider === 'codex' ? `codex-cli ${approved.cli_version}` : `${approved.cli_version} (Claude Code)`;
  if (version !== expected || fileDigest(approved.cli_path) !== approved.cli_sha256) {
    throw new Error('runtime_version_mismatch');
  }
  return Object.freeze({sdk_version: approved.sdk_version, sdk_sha256: approved.sdk_sha256,
    cli_path: approved.cli_path, cli_version: approved.cli_version, cli_sha256: approved.cli_sha256});
}
