/** Translate trusted supervisor-local paths into narrow Docker-owned volume mounts. */
import {lstatSync,realpathSync} from 'node:fs';
import {isAbsolute,relative,resolve,sep} from 'node:path';

export interface VolumeStorage {volume:string;root:string;}
export function validateVolumeStorage(storage:VolumeStorage|undefined):void {
  if(storage===undefined)return;
  if(!storage||Object.keys(storage).sort().join(',')!=='root,volume'||
    typeof storage.volume!=='string'||!/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$/.test(storage.volume)||
    typeof storage.root!=='string'||!isAbsolute(storage.root)||resolve(storage.root)!==storage.root||
    storage.root===sep||/[\0\r\n,]/.test(storage.root))throw new Error('invalid_volume_storage');
}

/** Only launch calls this: failed storage must never prevent identity-based stopping. */
export function storageMount(storage:VolumeStorage|undefined,source:string,target:string,readOnly:boolean):string {
  const suffix=readOnly?',readonly':'';
  if(!storage)return `type=bind,src=${source},dst=${target}${suffix}`;
  validateVolumeStorage(storage);
  const info=lstatSync(storage.root);
  if(!info.isDirectory()||info.isSymbolicLink()||info.uid!==process.getuid?.()||(info.mode&0o077)!==0)throw new Error('unsafe_volume_storage');
  const lexical=relative(storage.root,resolve(source));
  const actual=relative(realpathSync(storage.root),realpathSync(source));
  if(!lexical||lexical==='..'||lexical.startsWith('..'+sep)||isAbsolute(lexical)||lexical!==actual||
    /[\0\r\n,]/.test(lexical))throw new Error('volume_path_escape');
  return `type=volume,src=${storage.volume},dst=${target},volume-subpath=${actual},volume-nocopy${suffix}`;
}
