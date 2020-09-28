lsblk
service couchbase-server stop
#Get the LUKS Filesystem Name
LUKS_FS=$(cat /proc/mounts | grep /data | cut -d ' ' -f 1)
if [ "$LUKS_FS" != "/dev/mapper/cbefs" ] ; then
  echo "No LUKS Filesystem Found"
  exit 1
fi

DATA_PARTITION=$1

#Unmount /data partition
umount /data

#Close the lukspartition
sudo cryptsetup luksClose $LUKS_FS
status=$?
if [ "$status" -eq 0 ] ; then
  sed -i '/data/c\dev/sdb1 /data  xfs  defaults        0 0' /etc/fstab
fi
#Restore /data partition with XFS Format on
mkfs.xfs /dev/sdb1 -f

systemctl daemon-reload
mount /dev/sdb1 /data
status=$?
#Give couchbase user permissions to /data
chown couchbase:couchbase /data

lsblk