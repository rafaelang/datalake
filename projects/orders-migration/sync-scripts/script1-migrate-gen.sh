declare -a sequence

HEXPREFIX=$1
sequence=(0 1 2 3 4 5 6 7 8 9 a b c d e f)

mkdir scripts-migrate

for i in "${sequence[@]}"; do 
	for j in "${sequence[@]}"; do
		for k in "${sequence[@]}"; do
			for l in "${sequence[@]}"; do
				for m in "${sequence[@]}"; do
					echo aws s3 sync s3://vtex-orders-index/"$HEXPREFIX""$i""$j""$k""$l""$m"/ s3://vtex-analytics-import/vtex-orders-index/"$HEXPREFIX""$i""$j""$k""$l""$m"/ "&" >> migrate-$HEXPREFIX$i$j$k.sh
				done
			done
		done
	done
done


mv migrate-"$HEXPREFIX"* scripts-migrate