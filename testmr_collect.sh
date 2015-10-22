#!/usr/bin/env bash
echo "Testing mr_collect.py"
python mr_collect.py wordcount_out WordCount_collectresult
python mr_collect.py sort_out Sort_collectresult
python mr_collect.py hammingenc_out hammingEnc_collectresult
python mr_collect.py hammingfix_out hammingFix_collectresult
python mr_collect.py hammingchk_out hammingChk_collectresult
python mr_collect.py hammingdec_out hammingDec_collectresult
python mr_collect.py hammingerr_out hammingErr_collectresult

echo "Collect job finish"