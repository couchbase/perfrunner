#Script assumes there is a block device containing the /data partition already present and mounted on it
#/data partition is the data path for couchbase server which is set during the tests
systemctl daemon-reload
service couchbase-server stop
lsblk
LUKS_PARTITION_NAME="cbefs"
LUKS_KEYFILE="luks_keyfile.key"
LUKS_KEYFILE_PATH="~"
DATA_PARTITION=""
LUKS_KEY=$LUKS_KEYFILE_PATH/$LUKS_KEYFILE

#Check for existing LUKS format partitions
LUKS_PARTITION_EXISTS=$(lsblk | grep crypt)
if [ ! -z "$LUKS_PARTITION_EXISTS" ] ; then
  echo "Luks format partition exists : $LUKS_PARTITION_EXISTS"
  exit 1
fi

#Create a keyfile that stores the passphrase for the encrypted partition
#Passphrase is 'couchbase'
rm $LUKS_KEYFILE
echo "couchbase"  >> $LUKS_KEYFILE

#Check if cryptsetup is installed
INSTALLED=$(rpm -qa | grep cryptsetup)
if [ -z "$INSTALLED" ] ; then
  echo "Cryptsetup not installed"
  exit 1
fi

#Check if /data partition exists
PARTITION_EXISTS=$(lsblk | grep "part /data")
if [ -z "$PARTITION_EXISTS" ] ; then
  echo "The /data partition does not exist. Create and retry"
  exit 1
fi

#Obtain  which partition has the /data mounted
DATA_PARTITION=$(grep "/data " /proc/mounts | cut -d ' ' -f 1)
if [ -z "$DATA_PARTITION" ] ; then
  echo "/data partition not found on any block device"
  exit 1
fi
echo $DATA_PARTITION
#Unmount existing /data partition
umount /data
systemctl daemon-reload
#Luksformat the /data partition
sudo cryptsetup luksFormat -d luks_keyfile.key --batch-mode /dev/sdb1
sudo cryptsetup luksOpen -d luks_keyfile.key /dev/sdb1 cbefs
status=$?
#Create filesystem and mount /data to the new filesystem
if [ "$status" -eq 0 ] ; then
  mkfs.xfs /dev/mapper/cbefs
  mount /dev/mapper/cbefs /data
fi
status=$?

#Give couchbase user permissions to /data
chown couchbase:couchbase /data

#Add entries to fstab and crypttab
if [ "$status" -eq 0 ] ; then
  sed -i '/data/c\/dev/mapper/cbefs /data xfs defaults 0 2' /etc/fstab
  echo "cbefs /dev/sdb1 /root/luks_keyfile.key luks" > /etc/crypttab
fi

lsblk