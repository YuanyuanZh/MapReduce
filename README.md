# WZ-project2
Map Reduce implement
***************************************************************
Command to testing mr_seq.py

python mr_seq.py WordCount 128 2 wordcount.txt wordcount_out
python mr_seq.py Sort 256 2 sort.txt sort_out
python mr_seq.py hammingEnc 64 2 hammingenc.txt hammingenc_out
python mr_seq.py hammingDec 256 2 hammingdec.txt hammingdec_out
python mr_seq.py hammingFix 256 3 hammingfix.txt hammingfix_out
python mr_seq.py hammingChk 128 3 hammingchk.txt hammingchk_out
python mr_seq.py hammingErr 32 3 hammingerr.txt hammingerr_out 1

***************************************************************
Collect results from workers:
python mr_collect.py wordcount_out WordCount_result
python mr_collect.py sort_out Sort_result
python mr_collect.py hammingenc_out hammingEnc_result
python mr_collect.py hammingfix_out hammingFix_result
python mr_collect.py hammingchk_out hammingChk_result
python mr_collect.py hammingdec_out hammingDec_result
python mr_collect.py hammingerr_out hammingErr_result
