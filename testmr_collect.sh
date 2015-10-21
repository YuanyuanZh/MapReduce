#!/usr/bin/env bash
echo "Testing mr_collect.py"
python mr_collect.py wordcount_out WordCount_result
python mr_collect.py sort_out Sort_result
python mr_collect.py hammingenc_out hammingEnc_result
python mr_collect.py hammingfix_out hammingFix_result
python mr_collect.py hammingchk_out hammingChk_result
python mr_collect.py hammingdec_out hammingDec_result
python mr_collect.py hammingerr_out hammingErr_result

echo "Collect job finish"