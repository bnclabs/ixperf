for file in `ls -1 test_profiles/*`;
do
    echo "cargo run --release -- -v --profile $file"
    cargo run --release -- -v --profile $file
    ret=$?
    if [ $ret -ne 0 ]; then
        echo "exit code $ret"
        exit
    fi
done
