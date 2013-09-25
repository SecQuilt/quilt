#!/bin/bash
tag=$1

if [ -z "$tag" ] ; then
    echo "Tag required"
    exit 1
fi

# any second argument will be considered instruction to keep design docs
dldr=$2
if [ "$dldr" ] ; then
    dldr="_full"
fi


echo "Getting quilt ($tag) for counting purposes"
oldir=$(pwd)
cd /tmp
mkdir -p quiltcnt
cd quiltcnt
rm -rf quilt quilt_${tag}${dldr}
git clone git@romano:quilt
mv quilt quilt_${tag}${dldr}
cd quilt_${tag}${dldr}
git checkout $tag
cntdir=$(pwd)

if [ -z "$dldr" ] ; then
    echo Removing soft links
    find . -type l -exec rm -f {} \;
    echo Removing sweng files
    rm -rf sweng

    echo Removing text files
    find . -name "*.txt" -exec rm -f {} \;
    echo Removing test data files
    rm -rf test/var/log
    echo "Removing git files"
    rm -rf .git
fi
echo "Performing raw line count"
wc -l $(find . -type f) | tail -n 1
echo moving from counting location $cntdir to running location $oldir
mv $cntdir $oldir
