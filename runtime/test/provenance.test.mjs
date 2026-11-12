import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync, writeFileSync, rmSync, symlinkSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {fileDigest,packageDigest,installedPackage,verifyRuntime} from '../dist/provenance.js';
test('tree fingerprint changes with content and names and rejects redirected artifacts',t=>{
 const root=mkdtempSync(join(tmpdir(),'provenance-')); t.after(()=>rmSync(root,{recursive:true,force:true}));
 const file=join(root,'entry'); writeFileSync(file,'one'); const first=packageDigest(root);
 writeFileSync(file,'two'); assert.notEqual(packageDigest(root),first);
 const digest=fileDigest(file); symlinkSync(file,join(root,'redirect'));
 assert.throws(()=>packageDigest(root),/symlink/); assert.throws(()=>fileDigest(join(root,'redirect')));
 assert.match(digest,/^[a-f0-9]{64}$/);
});
test('approved CLI hash and version are checked against installed SDK before use',async t=>{
 const root=mkdtempSync(join(tmpdir(),'verified-cli-')); t.after(()=>rmSync(root,{recursive:true,force:true}));
 const cli=join(root,'codex'); writeFileSync(cli,'#!/bin/sh\necho "codex-cli 0.167.0"\n',{mode:0o700});
 const approval={provider:'codex',sdk_version:'0.167.0',sdk_sha256:packageDigest(installedPackage('codex')),
 cli_path:cli,cli_version:'0.167.0',cli_sha256:fileDigest(cli)};
 const binding=await verifyRuntime(approval); assert.equal(binding.cli_sha256,approval.cli_sha256);
 await assert.rejects(verifyRuntime({...approval,cli_sha256:'0'.repeat(64)}),/provenance_mismatch/);
 await assert.rejects(verifyRuntime({...approval,cli_version:'0.166.0'}),/version_mismatch/);
 await assert.rejects(verifyRuntime({...approval,sdk_version:'0.166.0'}),/provenance_mismatch/);
});
