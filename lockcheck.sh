#!/bin/bash

GOFILES=$(find . -name "*.go")
IGNORE="./daemon/inode.go"

FAILED=0
for f in $GOFILES
do
	if [[ "$f" =~ "_DOWN.go" ]]; then
		# Check to ensure that every function in _DOWN has the _DOWN suffix
		FUNCLINES=$(grep -P 'func (\(.*\) )?[a-zA-Z0-9]+(?!_DOWN)\(' $f)
		if [[ "$FUNCLINES" != "" ]]; then
			echo "Functions in $f without _DOWN suffix:"
			echo "$FUNCLINES"
			NUMLINES=$(echo "$FUNCLINES" | wc -l)
			FAILED=$((FAILED + NUMLINES))
		fi
	fi

	if ! [[ "$f" =~ "_DOWN.go" ]] && ! [[ $IGNORE =~ "$f" ]]; then
		# Check to ensure that every _DOWN call has a nearby Lock.Unlock
		CONTAINS=$(grep "_DOWN" $f)
		if [[ "$CONTAINS" != "" ]]; then
			WARNTEXT="_DOWN without LockTree().Unlock() in $f"

			unset FILEDATA
			declare -a FILEDATA
			while read -r LINE
			do
				FILEDATA+=("${LINE}")
			done < "$f"

			SAWDOWN=0
			LOCKLINE=-1
			ITER=$(cat $f | wc -l)
			while [ $ITER -ge 0 ]; do
				LINE=${FILEDATA[$ITER]}
				if [[ "$LINE" =~ "_DOWN" ]]; then
					LOCKLINE=$((ITER+1))
				fi
				if [[ ( "$LINE" =~ ".LockTree().Unlock()" ) || \
					( "$LINE" =~ ".LockTreeWaitAtMost(" ) ||\
					( "$LINE" =~ "func" && "$LINE" =~ "_(" ) ]]
					then
					if [ "$LOCKLINE" -ne "-1" ]; then
						LOCKLINE=-1
					fi
				fi
				if [[ "$LINE" =~ "func" ]]; then
					if [ "$LOCKLINE" -ne "-1" ]; then
						echo "$WARNTEXT, line $LOCKLINE"
						((FAILED++))
						LOCKLINE=-1
					fi
				fi
				((ITER--))
			done
		fi
	fi
done

if [[ "$FAILED" -ne "0" ]]; then
	echo -e "\n_DOWN lock check failed with $FAILED failures."
	exit 1
fi

exit 0
