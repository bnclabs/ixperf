for file in `ls -1 test_profiles`;
do
    if [ $? -eq 0 ] ; then
        echo "cargo run --release -- -v --profile profiles/$file | tee ixperf.log"
        cargo run --release -- -v --profile test_profiles/$file | tee ixperf.log
    fi
done
