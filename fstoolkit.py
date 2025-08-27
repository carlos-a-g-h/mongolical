#!/usr/bin/python3.9

from json import loads as json_loads
from pathlib import Path
from typing import Mapping,Optional,Union
from subprocess import run as sub_run

_RET_ALL=0
_RET_RETURNCODE=1
_RET_STDOUT=2

_PARTED_LABEL_MBR="msdos"
_PARTED_LABEL_GPT="gpt"

_FSTYPE_FAT32="fat32"
_FSTYPE_NTFS="ntfs"
_FSTYPE_EXFAT="exfat"
_FSTYPE_EXT4="ext4"

def util_fixstring(
		data:Optional[str],
		low:bool=False
	)->Optional[str]:

	if data is None:
		return None
	data=data.strip()
	if len(data)==0:
		return None
	if low:
		return data.lower()
	return data

def util_path_to_str(filepath)->str:
	fse_ok=filepath
	if isinstance(fse_ok,Path):
		fse_ok=str(fse_ok)
	return fse_ok

def util_subrun(
		command:list,
		ret_mode:int=0
	)->Union[tuple,int,Optional[str]]:


	#line=""
	#for p in command:
	#	if p.find(" ")==-1:
	#		line=f"{line}{p} "
	#		continue

	#	line=f"""{line}"{p}" """

	#print("\n$",line.strip())
	print("\n$",command)

	catch_output=(
		ret_mode in (_RET_ALL,_RET_STDOUT)
	)
	proc=sub_run(
		command,
		capture_output=catch_output,
		text=catch_output
	)

	if ret_mode==_RET_RETURNCODE:
		return proc.returncode

	output=util_fixstring(proc.stdout)
	if ret_mode==_RET_STDOUT:
		return output

	return (proc.returncode,output)

# BASIC

# MOUNTPOINT

def cmd_mountpoint(
		dirpath:Union[str,Path],
	)->Union[bool,Optional[str]]:

	# Returns wether a directory is a mountpoint or not

	dir_str=util_path_to_str(dirpath)

	result=util_subrun(["mountpoint",dir_str])
	if not result[0]==0:
		if result[1] is not None:
			print(result[1])
		return False

	return True

# MOUNT

def cmd_mount_path(
		orig:Union[str,Path],
		dest:Union[str,Path],
		spec_mode:Optional[str]=None,
		ensure_dest:bool=False,
		conf_only:bool=True,
	)->Union[bool,int]:

	# Mounts a block device (filesystem) or a directory (bind mount) depending on the path given

	fse_dev=util_path_to_str(orig)
	fse_dir=util_path_to_str(dest)

	if ensure_dest:
		Path(fse_dir).mkdir(
			exist_ok=True,
			parents=True
		)

	command=["mount"]
	if Path(fse_dev).is_dir():
		command.append("-B")

	if spec_mode in ("rw","ro","auto"):
		command.extend(["-o",spec_mode])
	command.extend([fse_dev,fse_dir])

	result=util_subrun(command)
	if not result[0]==0:
		if result[1] is not None:
			print(result[1])

		if not conf_only:
			return result[0]

		return False

	if not conf_only:
		return result[0]

	return True

def cmd_mount_volume(
		uuid:str,
		dest:str,
		spec_mode:Optional[str]=None,
		ensure_dest:bool=False,
		conf_only:bool=True,
	)->Union[bool,int]:

	# mounts a volume with a known UUID

	fse_dir=util_path_to_str(dest)

	if ensure_dest:
		Path(fse_dir).mkdir(
			exist_ok=True,
			parents=True
		)

	command=["mount"]
	if spec_mode in ("rw","ro","auto"):
		command.extend(["-o",spec_mode])
	command.extend(["--uuid",uuid,fse_dir])

	result=util_subrun(command)
	if not result[0]==0:
		if result[1] is not None:
			print(result[1])

		if not conf_only:
			return result[0]

		return False

	if not conf_only:
		return result[0]

	return True

# UMOUNT

def cmd_umount(
		mount_point:Union[str,Path],
		recursive:bool=False,
		conf_only:bool=True,
	)->Union[bool,int]:

	# Unmounts whatever is mounted on a given directory

	fse_dir=util_path_to_str(mount_point)
	if not Path(fse_dir).is_dir():
		if not conf_only:
			return 69
		return False

	command=["umount"]
	if recursive:
		command.append("-R")
	command.append(fse_dir)

	result=util_subrun(command)
	if not result[0]==0:
		if result[1] is not None:
			print(result[1])

		if not conf_only:
			return result[0]

		return False

	if not conf_only:
		return result[0]

	return True

# FINDMNT

def cmd_findmnt_get_filesystems(
		filepath:Union[str,Path],
		exclude_itself:bool=False,
		raw_json:bool=False
	)->Union[Mapping,list]:

	# Finds all the mountpoints that come from the given device or a device through its mountpoint

	fse_ok=util_path_to_str(filepath)

	result=util_subrun([
		"findmnt","-J",
		"-o","SOURCE,FSROOT,TARGET",
		fse_ok
	])

	if not result[0]==0:
		if result[1] is not None:
			print(result[1])
		if not raw_json:
			return []
		return {}

	if result[1] is None:
		print("No output...?")
		if not raw_json:
			return []
		return {}

	print(result[1])

	data={}
	try:
		data.update(
			json_loads(
				result[1].strip()
			)
		)
	except Exception as exc:
		print("Unhandled exception:",exc)
		if not raw_json:
			return []
		return {}

	if raw_json:
		return data

	filesystems_list=data.get("filesystems")
	if not isinstance(filesystems_list,list):
		return []

	if len(filesystems_list)==0:
		return []

	selection=[]
	for item in filesystems_list:
		if not isinstance(item,Mapping):
			continue
		if exclude_itself:
			if item.get("source")==fse_ok:
				continue
		selection.append(item)

	return selection

# LSBLK

def cmd_lsblk_get_devices(

		filepath:Union[str,Path],

		# Columns
			inc_mountpoint:bool=False,
			inc_uuid:bool=False,
			inc_all_types:bool=False,
			inc_all_sizes:bool=False,
			inc_all_labels:bool=False,
			inc_brand_info:bool=False,
			custom_cols:Optional[str]=None,

		exclude_itself:bool=False,
		get_quantity:bool=False,
		raw_json:bool=False

	)->Union[Mapping,list]:

	# Get all devices related to a device (partitions for example)

	fse_ok=util_path_to_str(filepath)

	columns="PATH"
	if custom_cols is None:
		if inc_uuid:
			columns=f"{columns},UUID"
		if inc_mountpoint:
			columns=f"{columns},MOUNTPOINT"
		if inc_all_types:
			columns=f"{columns},TYPE,FSTYPE"
		if inc_all_sizes:
			columns=f"{columns},SIZE,FSSIZE"
		if inc_brand_info:
			columns=f"{columns},VENDOR,MODEL,SERIAL,REV"

	if custom_cols is not None:
		columns=custom_cols

	command=[
		"lsblk",fse_ok,
			"--paths",
			"--json",
	]
	if inc_all_sizes:
		command.append("-b")
	command.extend(["--output",columns])

	result=util_subrun(command)
	if not result[0]==0:
		if result[1] is not None:
			print(result[1])
		if get_quantity:
			return -1
		if raw_json:
			return {}
		return []

	if result[1] is None:
		print("No output...?")
		if get_quantity:
			return 0
		if raw_json:
			return {}
		return []

	print(result[1])

	data={}
	try:
		data.update(
			json_loads(
				result[1].strip()
			)
		)
	except Exception as exc:
		print("Unhandled exception:",exc)
		if get_quantity:
			return -1
		if raw_json:
			return {}
		return []

	if raw_json:
		return data

	blockdevices_list=data.get("blockdevices")
	if not isinstance(blockdevices_list,list):
		if get_quantity:
			return -1
		return []

	if len(blockdevices_list)==0:
		if get_quantity:
			return 0
		return []

	selection=[]

	for item in blockdevices_list:
		if not isinstance(item,Mapping):
			continue
		if exclude_itself:
			if item.get("path")==fse_ok:
				continue

		selection.append(item)

	if get_quantity:
		return len(selection)
	return selection

def cmd_lsblk_get_dev_size(
		filepath:Union[str,Path],
		is_filesystem:bool=False
	)->Optional[int]:

	# Get disk/filesystem size

	column={
		True:"FSSIZE",
		False:"SIZE"
	}[is_filesystem]

	fse_ok=util_path_to_str(filepath)

	result=util_subrun([
		"lsblk",fse_ok,
		"-n","-b",
		"-o",column,
	])
	if not result[0]==0:
		print(result[1])
		return None

	size_raw=result[1]
	if not size_raw.isdigit():
		print("NaN")
		return None

	return int(size_raw)

# PARTED

def cmd_parted_disk_init(
		filepath:Union[str,Path],
		table:str
	)->bool:

	# Creates a partition table

	fse_ok=util_path_to_str(filepath)

	result=util_subrun([
		"parted","-s",fse_ok,
		"mklabel",table
	])
	if not result[0]==0:
		if result[1] is not None:
			print(result[1])

	return (result[0]==0)

def cmd_parted_part_new(
		filepath:Union[str,Path],
		fs_type:str,
		fs_start:Optional[str]=None,
		fs_end:Optional[str]=None,
	)->bool:

	# Create a primary partition

	fse_ok=util_path_to_str(filepath)

	the_start=fs_start
	the_end=fs_end
	if fs_type==_FSTYPE_EXT4:
		if the_start is None:
			the_start="1MiB"
		if the_end is None:
			the_end="100%"

	if fs_type==_FSTYPE_FAT32:
		if the_start is None:
			the_start="2048s"
		if the_end is None:
			the_end="100%"

	result=util_subrun([
		"parted","-s",fse_ok,
		"mkpart","primary",fs_type,
		the_start,the_end
	])
	if not result[0]==0:
		if result[1] is not None:
			print(result[1])

	return (result[0]==0)

# MKFS

def cmd_mkfs_part_format(
		filepath:Union[str,Path],
		fs_type:str,
		fs_label:Optional[str]=None,
	)->bool:

	# Formats a partition

	command=[]
	fs_label_ok=util_fixstring(fs_label,low=True)
	if fs_type==_FSTYPE_EXT4:
		command.extend(["mkfs.ext4","-F"])
		if fs_label_ok is not None:
			command.extend(["-L",fs_label_ok])

	if fs_type==_FSTYPE_FAT32:
		command.extend(["mkfs.fat","-v","-F","32"])
		if fs_label_ok is not None:
			command.extend(["-n",fs_label_ok])

	command.append(
		util_path_to_str(filepath)
	)

	result=util_subrun(command)
	if not result[0]==0:
		if result[1] is not None:
			print(result[1])

	return (result[0]==0)

# LOSETUP

def cmd_losetup_get_devices(
		filepath:Union[str,Path],
		# Columns
			inc_backfile:bool=False,
			inc_ro:bool=False,
			inc_all_geometry:bool=False,
			inc_all_inode:bool=False,
			custom_cols:Optional[str]=None,

		exclude_itself:bool=False,
		get_quantity:bool=False,
		raw_json:bool=False

	)->Union[int,list,Mapping]:

	# Given a path to a file, it gets all the loop devices that come from that file
	# Returns a lists of paths

	fse_ok=util_path_to_str(filepath)

	columns="NAME"
	if custom_cols is None:
		if inc_backfile:
			columns=f"{columns},BACK-FILE"
		if inc_ro:
			columns=f"{columns},RO"
		if inc_all_geometry:
			columns=f"{columns},SIZELIMIT,OFFSET"
		if inc_all_inode:
			columns=f"{columns},BACK-INO,BACK-MAJ:MIN"

	if custom_cols is not None:
		columns=custom_cols

	result=util_subrun([
		"losetup",
		"--list",
		"--json",
		"--associated",fse_ok,
		"--output",columns,
	])
	if not result[0]==0:
		if result[1] is not None:
			print(result[1])
		if get_quantity:
			return -1
		if raw_json:
			return {}
		return []

	if result[1] is None:
		print("No output...?")
		if get_quantity:
			return 0
		if raw_json:
			return {}
		return []

	print(result[1])

	data={}
	try:
		data.update(
			json_loads(
				result[1].strip()
			)
		)
	except Exception as exc:
		print("Unhandled exception:",exc)
		if get_quantity:
			return -1
		if raw_json:
			return {}
		return []

	if raw_json:
		return data

	loopdevices_list=data.get("loopdevices")
	if not isinstance(loopdevices_list,list):
		if get_quantity:
			return -1
		return []

	if len(loopdevices_list)==0:
		if get_quantity:
			return 0
		return []

	selection=[]

	for item in loopdevices_list:
		if not isinstance(item,Mapping):
			continue
		if exclude_itself:
			if item.get("name")==fse_ok:
				continue

		selection.append(item)

	if get_quantity:
		return len(selection)
	return selection

def cmd_losetup_attach(
		filepath:Union[str,Path],
		get_as_pl:bool=False,
		partitioned:bool=False,
	)->Optional[Union[str,Path]]:

	# Given a path to a file, finds and attaches a loop device to it
	# Returns the path to the loop device if successful

	fse_ok=util_path_to_str(filepath)

	command=["losetup"]

	if partitioned:
		command.append("--partscan")

	command.extend(["--find",fse_ok,"--show"])

	result=util_subrun(command)

	if not result[0]==0:
		if result[1] is not None:
			print(result[1])
			return None

	if get_as_pl:
		return Path(result[1])

	return result[1]

def cmd_losetup_detatch(
		filepath:Union[str,Path],
		detach_all:bool=False
	)->bool:

	# Detaches a loop device

	d="--detach"
	if detach_all:
		d=f"{d}-all"

	fse_ok=util_path_to_str(filepath)
	result=util_subrun([
			"losetup",
			d,fse_ok
		],
		ret_mode=_RET_RETURNCODE
	)
	return result==0

# HIGH LEVEL

def fun_recursive_unmount(filepath:Union[str,Path])->bool:

	# Given a path to a source, it finds and unmounts everything that is on top of it

	fs_list=cmd_findmnt_get_filesystems(filepath)

	count=0
	count_max=len(fs_list)

	father:Optional[str]=None

	fse_ok=util_path_to_str(filepath)

	for fs in fs_list:

		if not isinstance(fs,Mapping):
			continue

		if father is None:
			if fs.get("source")==fse_ok:
				father=fs.get("target")
				continue

		print(fs)

		fs_target=fs.get("target")
		if cmd_umount(fs_target):
			count=count+1

	if father is not None:
		if cmd_umount(father):
			count=count+1

	return count==count_max

def fun_unmount_all_parts(filepath:Union[str,Path])->bool:

	# Given a path to a block device, it unmounts all of its partitions

	list_of_bdevs=cmd_lsblk_get_devices(
		filepath,
		inc_mountpoint=True
	)

	count=0
	count_max=0

	for bdev in list_of_bdevs:
		if not isinstance(bdev,Mapping):
			continue

		print(
			"\nUnmounting:",
			bdev
		)
		mpoint=util_fixstring(bdev.get("mountpoint"))
		if mpoint is None:
			continue

		bdev_path=util_fixstring(bdev.get("path"))
		if bdev_path is None:
			continue

		count_max=count_max+1
		if not fun_recursive_unmount(bdev_path):
			continue

		count=count+1

	return (count==count_max)

def fun_deep_detatch(
		filepath:Union[str,Path],
		verbose:bool=True
	)->bool:

	# Given a path to a file, it does the following:
	# → gets all loop devices that come from the file
	# → for each loop device detected, unmount them completely

	fse_ok=util_path_to_str(filepath)

	loopdev_list=cmd_losetup_get_devices(fse_ok)
	if len(loopdev_list)==0:
		return True

	count=0
	count_max=len(loopdev_list)

	for loopdev in loopdev_list:

		if not isinstance(loopdev,Mapping):
			continue

		print(
			"\nPerforming full detach on:",
			loopdev
		)

		loopdev_path=loopdev.get("name")

		if not fun_unmount_all_parts(loopdev_path):
			continue

		if not cmd_losetup_detatch(loopdev_path):
			continue

		count=count+1

	return (count==count_max)

def fun_create_and_format_part(
		filepath:Union[str,Path],
		fs_type:str,
		fs_label:Optional[str]=None,
		fs_start:Optional[str]=None,
		fs_end:Optional[str]=None,
		conf_only:bool=False
	)->Union[bool,Optional[str]]:

	# Creates a new partition, finds it, and formats it

	fse_ok=util_path_to_str(filepath)

	parts_before=cmd_lsblk_get_devices(
		fse_ok,
		exclude_itself=True
	)

	if not cmd_parted_part_new(
			filepath,fs_type,
			fs_start=fs_start,
			fs_end=fs_end,
		):

		if conf_only:
			return False
		return None

	parts_after=cmd_lsblk_get_devices(
		fse_ok,
		exclude_itself=True
	)
	if not len(parts_after)==len(parts_before)+1:
		print("Only ONE new partition should be here")
		if conf_only:
			return False
		return None

	parts=[]
	for p in parts_before:
		if not isinstance(p,Mapping):
			continue
		pp=p.get("path")
		if pp is None:
			continue
		parts.append(pp)

	fse_part:Optional[str]=None

	for q in parts_after:
		if not isinstance(q,Mapping):
			continue
		qq=q.get("path")
		if qq is None:
			continue

		if fse_part not in parts:
			fse_part=qq
			break

	if fse_part is None:
		print("Partition not found")
		if conf_only:
			return False
		return None

	if not cmd_mkfs_part_format(
			fse_part,
			fs_type,
			fs_label=fs_label
		):

		if conf_only:
			return False
		return None

	if conf_only:
		return True
	return fse_part

# if __name__=="__main__":

# 	pass

# 	# fun_deep_detatch("/root/test.disk")
# 	# fun_unmount_all_parts("/dev/sda2")
