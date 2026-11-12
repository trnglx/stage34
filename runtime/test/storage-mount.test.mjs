import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,mkdirSync,rmSync,symlinkSync,chmodSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {storageMount,validateVolumeStorage} from '../dist/storage-mount.js';
function fixture(t){const root=mkdtempSync(join(tmpdir(),'volume-mount-'));t.after(()=>rmSync(root,{recursive:true,force:true}));return root;}
test('volume mapping exposes only the requested existing subdirectory',t=>{
 const root=fixture(t);const entry=join(root,'boots','B-123','entry');mkdirSync(entry,{recursive:true,mode:0o700});
 assert.equal(storageMount({volume:'worker-state',root},entry,'/run/orch-entry',true),
  'type=volume,src=worker-state,dst=/run/orch-entry,volume-subpath=boots/B-123/entry,volume-nocopy,readonly');
 assert.equal(storageMount(undefined,entry,'/run/orch-entry',true),`type=bind,src=${entry},dst=/run/orch-entry,readonly`);
});
test('mapping refuses volume-root exposure, escapes and symlink redirection',t=>{
 const root=fixture(t),outside=fixture(t);mkdirSync(join(root,'private'),{mode:0o700});
 const storage={volume:'worker-state',root};symlinkSync(outside,join(root,'escape'));symlinkSync(join(root,'private'),join(root,'alias'));
 for(const path of [root,outside,join(root,'escape'),join(root,'alias')])assert.throws(()=>storageMount(storage,path,'/workspace',false),/volume_path_escape/);
});
test('volume configuration refuses mount option injection and writable roots',t=>{
 const root=fixture(t);
 for(const storage of [{volume:'name,readonly',root},{volume:'worker',root:'/'},{volume:'worker',root:root+'/../other'},{volume:'worker',root,extra:true}])assert.throws(()=>validateVolumeStorage(storage),/invalid_volume_storage/);
 chmodSync(root,0o777);assert.throws(()=>storageMount({volume:'worker',root},root+'/child','/workspace',false),/unsafe_volume_storage/);chmodSync(root,0o700);
});
