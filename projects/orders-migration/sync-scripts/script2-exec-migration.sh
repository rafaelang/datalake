declare -a sequence

HEXPREFIX=$1
sequence=(0 1 2 3 4 5 6 7 8 9 a b c d e f)

for i in "${sequence[@]}"; do 
	for j in "${sequence[@]}"; do
		for k in "${sequence[@]}"; do
			echo "bash ./scripts-migrate/migrate-""$HEXPREFIX""$i""$j""$k"".sh >> output""$HEXPREFIX"" 2>>output""$HEXPREFIX""-err" >> sync-"$HEXPREFIX".sh;		
		done
	done
done
