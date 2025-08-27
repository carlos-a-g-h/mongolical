#!/usr/bin/python3.9

from secrets import token_hex

from typing import Mapping,Optional,Union

from pathlib import Path

from fstoolkit import (

	_PARTED_LABEL_GPT,
	_PARTED_LABEL_MBR,
	_FSTYPE_EXT4,

	util_fixstring,
	util_path_to_str,
	util_subrun,

	cmd_mountpoint,
	cmd_mount_path,
	cmd_losetup_attach,
	cmd_parted_disk_init,
	cmd_lsblk_get_devices,
	cmd_losetup_get_devices,
	cmd_losetup_detatch,
	cmd_findmnt_get_filesystems,

	fun_create_and_format_part,
	fun_deep_detatch,
)

_LABEL="MongoDB Stuff"

_ERR="error"

_CMD_NEW="new"
_CMD_MOUNT="mount"
_CMD_SETUP="setup"
_CMD_CLEAN="clean"

_RET_ALL=0
_RET_RETURNCODE=1
_RET_STDOUT=2

_DIR_MOUNT_DEFAULT="/mnt/mongodb"
_DIR_MOUNT_DATA="mongo-data"
_DIR_MOUNT_LOGS="mongo-logs"
_DIR_DEFAULT_DATA="/var/lib/mongodb"
_DIR_DEFAULT_LOGS="/var/log/mongodb"

# TODO:
# On reboot, the partition dissapears

_FLAG_SETUP="setup"
_FLAG_MOUNT="mount"
_FLAG_DESTROY="destroy"
_FLAG_TEST="test"

_ARG_OFILE="--file"
_ARG_MTARGET="--target"
_ARG_SIZE="--size"
_ARG_MONGO_DATA="--path-data"
_ARG_MONGO_LOGS="--path-logs"
_ARG_FLAGS="--flags"

def util_extract_pargs(command:str,args:list)->Mapping:

	args_allowed=[]
	if command==_CMD_NEW:
		args_allowed.extend([
			_ARG_OFILE,
			_ARG_MTARGET,
			_ARG_SIZE,
			_ARG_MONGO_DATA,
			_ARG_MONGO_LOGS,
			_ARG_FLAGS
		])
	if command==_CMD_MOUNT:
		args_allowed.extend([
			_ARG_OFILE,
			_ARG_MTARGET,
			_ARG_MONGO_DATA,
			_ARG_MONGO_LOGS,
			_ARG_FLAGS
		])
	if command==_CMD_SETUP:
		args_allowed.extend([
			_ARG_OFILE,
			_ARG_MTARGET,
			_ARG_MONGO_DATA,
			_ARG_MONGO_LOGS,
			_ARG_FLAGS
		])
	if command==_CMD_CLEAN:
		args_allowed.extend([
			_ARG_OFILE,
			_ARG_MTARGET,
			_ARG_FLAGS
		])

	pargs={}

	idx=0
	size=len(args)
	while True:
		if idx+1>size-1:
			break

		key=util_fixstring(args[idx],low=True)
		if key is None:
			idx=idx+2
			continue
		if key not in args_allowed:
			idx=idx+2
			continue

		value=util_fixstring(args[idx+1])
		if value is None:
			idx=idx+2
			continue

		pargs.update({key:args[idx+1]})
		idx=idx+2

	return pargs

def util_extract_flags(raw:str)->list:

	fil=[
		_FLAG_TEST,
		_FLAG_DESTROY,
		_FLAG_MOUNT,
		_FLAG_SETUP
	]

	split_raw=raw.split(":")
	ok=[]
	for x in split_raw:
		xx=util_fixstring(x,low=True)
		if xx is None:
			continue
		if xx not in fil:
			continue
		ok.append(xx)

	return ok

def util_msg_err(text:str,details:Optional[str]=None)->str:
	msg=text
	if details is not None:
		msg=(
			f"{text}\n"
			"Details:\n"
			f"{details}"
		)

	return msg

def util_fixpath(basedir:Path,fpath_str:str)->Path:

	if not fpath_str.startswith(":"):
		f=Path(fpath_str)
		if f.is_absolute():
			return f

		return f

	return basedir.joinpath(
		Path(fpath_str[1:])
	)

def fsutil_mount_path(
		orig:str,
		dest:Union[str,Path],
		ensure_dest:bool=False,
	)->bool:

	if Path(dest).exists():
		if cmd_mountpoint(dest):
			return True

	return (
		cmd_mount_path(
			orig,dest,
			spec_mode="rw",
			ensure_dest=ensure_dest
		)
	)

def fsutil_attach_as_loopdevice(fse:str)->tuple:

	if not cmd_losetup_get_devices(fse,get_quantity=True)==0:
		return (_ERR,"the file is already attached")

	fse_ok=cmd_losetup_attach(fse,partitioned=True)
	if fse_ok is None:
		return (_ERR,"failed to attach as a loop device")

	return tuple([fse_ok])

def main_create(
		filepath:Path,
		file_size:str,
		mountpoint:Path
	)->Optional[str]:

	# Creates a raw disk image with an MBR partition table and a single partition

	filepath_str=str(filepath)
	filepath.parent.mkdir(
		exist_ok=True,
		parents=True
	)

	exists=filepath.exists()
	if exists:
		if not filepath.is_file():
			return "the path is already occupied and not by a file"

	if not exists:
		result=util_subrun([
			"truncate",
			"-s",file_size,
			filepath_str,
		],ret_mode=_RET_RETURNCODE)
		if not result==0:
			return "failed to create the initial file"

	res=fsutil_attach_as_loopdevice(filepath_str)
	if res[0]==_ERR:
		return res[1]
	fse_loopdev=res[0]

	if not cmd_parted_disk_init(fse_loopdev,_PARTED_LABEL_MBR):
		return "failed to create partition table"

	fse_part=fun_create_and_format_part(
		fse_loopdev,
		_FSTYPE_EXT4,
		fs_label=_LABEL
	)
	if fse_part is None:
		return "failed to create and format partition"

	if not fsutil_mount_path(
			fse_part,
			mountpoint,
			ensure_dest=True
		):
		return "failed to mount the partition"

	dirlist=[
		mountpoint,
		mountpoint.joinpath("data"),
		mountpoint.joinpath("logs")
	]

	ok=True
	for dir in dirlist:
		Path(dir).mkdir(
			parents=True,
			exist_ok=True
		)
		result=util_subrun([
			"chown",
				"mongodb:mongodb",
				"-R",
				str(dir)
			],
			ret_mode=_RET_RETURNCODE
		)
		if not result==0:
			ok=False
			break

	if not ok:
		return f"failed to adjust ownership for: {dir}"

	return None

def main_mount(
		filepath:Path,
		mpoint:Path
	)->Optional[str]:


	fse_loopdev=None
	loop_devices=cmd_losetup_get_devices(filepath)
	attached=len(loop_devices)==1
	if attached:
		print("NOTE: the file is already attached")
		fse_loopdev=loop_devices[0].get("name")
	if not attached:
		res=fsutil_attach_as_loopdevice(filepath)
		if res[0]==_ERR:
			return res[1]
		fse_loopdev=res[0]

	lst=cmd_lsblk_get_devices(fse_loopdev,exclude_itself=True)
	if not len(lst)>0:
		return "there are no partitions"

	if not isinstance(lst[0],Mapping):
		return util_msg_err(
			"object not valid",
			f"{lst[0]}"
		)

	fse_part=util_fixstring(lst[0].get("path"))
	if fse_part is None:
		return "partition not found...?"

	if not fsutil_mount_path(
			fse_part,mpoint,
			ensure_dest=True
		):
		return "failed to mount the partition"

	return None

def main_setup(
		filepath:Path,
		mongo_data:Path,
		mongo_logs:Path
	)->Optional[str]:

	devices=cmd_losetup_get_devices(filepath)
	qtty=len(devices)
	if not qtty==1:
		return util_msg_err(
			"only ONE associated file is needed",
			f"there is(are) {qtty} device(s)"
		)

	loopdev=devices[0].get("name")

	parts=cmd_lsblk_get_devices(
		loopdev,
		inc_mountpoint=True,
		exclude_itself=True
	)
	qtty=len(parts)
	if qtty==0:
		return "at least ONE partition should be here"

	fse_mpoint=parts[0].get("mountpoint")
	if fse_mpoint is None:
		fse_part=parts[0].get("path")
		fse_mpoint=Path(_DIR_MOUNT_DEFAULT)
		if not cmd_mount_path(
			fse_part,
			fse_mpoint
		):
			return "failed to mount"

	m_data=Path(fse_mpoint).joinpath("data")
	m_logs=Path(fse_mpoint).joinpath("logs")

	x=[
		(m_data,mongo_data),
		(m_logs,mongo_logs)
	]

	msg_err:Optional[str]=None

	for pair in x:
		if not fsutil_mount_path(pair[0],pair[1]):
			msg_err=util_msg_err(
				"failed to mount",
				f"orig:{pair[0]}\ndest:{[pair[1]]}"
			)
			break

	return msg_err

def main_clean(filepath:Path)->Optional[str]:

	# Given a path to a regular file, checks for any loop devices it is linked to, and it detatches the file from them

	if not fun_deep_detatch(filepath):

		return "failed to detatch from loopback device(s)"

	return None

if __name__=="__main__":

	from sys import (
		argv as sys_argv,
		exit as sys_exit
	)

	if not len(sys_argv)>2:
		print(
			"\n- MONGOLICAL -"
			f"\nCommands: {[_CMD_NEW,_CMD_MOUNT,_CMD_SETUP,_CMD_CLEAN]}"
		)
		sys_exit(0)

	cmd=util_fixstring(sys_argv[1],low=True)
	pos_args=util_extract_pargs(cmd,sys_argv[2:])
	flags=[]
	if pos_args.get(_ARG_FLAGS) is not None:
		flags.extend(
			util_extract_flags(
				pos_args.get(_ARG_FLAGS)
			)
		)

	filepath:Optional[Path]=None
	basedir=Path(sys_argv[0]).parent

	then_mount=False
	then_setup=False
	then_clean=False
	then_destroy=False

	path_mpoint:Optional[Path]=None
	path_mongo_data:Optional[Path]=None
	path_mongo_logs:Optional[Path]=None

	flags=[]

	if cmd==_CMD_NEW:

		print("\n- Creating new virtual disk")

		filepath=util_fixpath(
			basedir,
			pos_args[_ARG_OFILE]
		)
		file_size=pos_args[_ARG_SIZE]

		if _ARG_MTARGET in pos_args.keys():
			path_mpoint=util_fixpath(
				basedir,
				pos_args[_ARG_MTARGET]
			)

		if _ARG_MONGO_DATA in pos_args.keys():
			path_mongo_data=util_fixpath(
				basedir,
				pos_args[_ARG_MONGO_DATA]
			)
			then_mount=True
			then_setup=True

		if _ARG_MONGO_LOGS in pos_args.keys():
			path_mongo_logs=util_fixpath(
				basedir,
				pos_args[_ARG_MONGO_LOGS]
			)
			then_mount=True
			then_setup=True

		if _ARG_FLAGS in pos_args.keys():
			flags.extend(
				util_extract_flags(
					pos_args[_ARG_FLAGS]
				)
			)

		if _FLAG_SETUP in flags:
			then_setup=True

		if _FLAG_TEST in flags:
			then_mount=True
			then_setup=True
			then_clean=True
			then_destroy=True

		if path_mpoint is None:
			path_mpoint=Path(_DIR_MOUNT_DEFAULT)

		print(
			"\nParameters:"
			f"\nFilepath: {str(filepath)}"
			f"\nFile size: {file_size}"
			f"\nMountpoint: {path_mpoint}"
		)

		msg_err=main_create(
			filepath,
			file_size,
			path_mpoint
		)
		if msg_err is not None:
			print(f"\n{msg_err}")

	if cmd==_CMD_MOUNT or then_mount or then_setup:

		print("\n- Mounting virtual disk")

		if cmd==_CMD_MOUNT:

			filepath=util_fixpath(
				basedir,
				pos_args[_ARG_OFILE]
			)

			if _ARG_MTARGET in pos_args.keys():
				path_mpoint=util_fixpath(
					basedir,
					pos_args[_ARG_MTARGET]
				)

			if _ARG_MONGO_DATA in pos_args.keys():
				path_mongo_data=util_fixpath(
					basedir,
					pos_args[_ARG_MONGO_DATA]
				)
				then_setup=True

			if _ARG_MONGO_LOGS in pos_args.keys():
				path_mongo_logs=util_fixpath(
					basedir,
					pos_args[_ARG_MONGO_LOGS]
				)
				then_setup=True

			if _ARG_FLAGS in pos_args.keys():
				flags.extend(
					util_extract_flags(
						pos_args[_ARG_FLAGS]
					)
				)

			if _FLAG_SETUP in flags:
				then_setup=True

			if _FLAG_TEST in flags:
				then_setup=True
				then_clean=True

		if path_mpoint is None:
			path_mpoint=Path(_DIR_MOUNT_DEFAULT)

		print(
			"\nParameters:"
			f"\nFilepath: {str(filepath)}"
			f"\nMountpoint: {str(path_mpoint)}"
		)

		msg_err=main_mount(
			filepath,
			path_mpoint
		)
		if msg_err is not None:
			print(f"\n{msg_err}")

	if cmd==_CMD_SETUP or then_setup:

		print("\n- Setting up bind mounts from the partition to the specified directories")

		if cmd==_CMD_SETUP:

			filepath=util_fixpath(
				basedir,
				pos_args[_ARG_OFILE]
			)
			if _ARG_MONGO_DATA in pos_args.keys():
				path_mongo_data=util_fixpath(
					basedir,
					pos_args[_ARG_MONGO_DATA]
				)
			if _ARG_MONGO_LOGS in pos_args.keys():
				path_mongo_logs=util_fixpath(
					basedir,
					pos_args[_ARG_MONGO_LOGS]
				)
			if _ARG_FLAGS in pos_args.keys():
				flags.extend(
					util_extract_flags(
						pos_args[_ARG_FLAGS]
					)
				)

			if _FLAG_TEST in flags:
				then_clean=True

		if path_mongo_data is None:
			path_mongo_data=Path(_DIR_DEFAULT_DATA)
		if path_mongo_logs is None:
			path_mongo_logs=Path(_DIR_DEFAULT_LOGS)

		print(
			"\nParameters:"
			f"\nFilepath: {str(filepath)}"
			f"\nMongoDB Data: {str(path_mongo_data)}"
			f"\nMongoDB Logs: {str(path_mongo_logs)}"
		)

		msg_err=main_setup(
			filepath,
			path_mongo_data,
			path_mongo_logs
		)
		if msg_err is not None:
			print(f"\n{msg_err}")

	if cmd==_CMD_CLEAN or then_clean:

		if cmd==_CMD_CLEAN:

			filepath=util_fixpath(
				basedir,
				pos_args[_ARG_OFILE]
			)
			if _ARG_FLAGS in pos_args.keys():
				flags.extend(
					util_extract_flags(
						pos_args[_ARG_FLAGS]
					)
				)

			then_destroy=(_FLAG_DESTROY in flags)

		msg_err=main_clean(filepath)
		if msg_err is not None:
			print(f"\n{msg_err}")

		if then_destroy and (msg_err is None):
			filepath.unlink()
			if not filepath.exists():
				print("\nFILE DESTROYED")

	print("\nEND OF PROGRAM\n")
