echo "Testing mr_seq.py"

python mr_seq.py WordCount 128 2 wordcount.txt wordcount_out
echo "Finish word counting"

python mr_seq.py Sort 256 2 sort.txt sort_out
echo "Finish word sorting"


python mr_seq.py hammingEnc 64 2 hammingenc.txt hammingenc_out
echo "Finish Hamming Encoding"
python mr_seq.py hammingDec 256 2 hammingdec.txt hammingdec_out
echo "Finish Hamming Decoding"

diff ./testoutput/hammingEnc_result.txt ./test/hammingdec.txt
res=$?
if [ $res -eq 0 ]; then
    echo "[PASS] hamming encoding and decoding"
else
    echo "[FAIL] hamming encoding and decoding"
fi


python mr_seq.py hammingFix 256 3 hammingfix.txt hammingfix_out
echo "Finish Hamming Fix"

diff ./testoutput/hammingEnc_result.txt ./testoutput/hammingFix_result.txt
res=$?
if [ $res -eq 0 ]; then
    echo "[PASS] hamming error Fix"
else
    echo "[FAIL] hamming error fix"
fi

python mr_seq.py hammingChk 128 3 hammingchk.txt hammingchk_out
echo "Finish Hamming Error create"
python mr_seq.py hammingErr 32 3 hammingerr.txt hammingerr_out 1


echo "ALL seq test finished"
