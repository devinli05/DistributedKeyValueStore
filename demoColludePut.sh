#!/bin/bash
echo "trial 3"
bash tearNodes.sh
bash setupNodes.sh
sleep 6
bash localhostColludePutPut.sh
sleep 1
cat regex.txt > shivizCollude3.log
cat HttpNodeLog0-Log.txt >> shivizCollude3.log
cat HttpNodeLog1-Log.txt >> shivizCollude3.log
cat 0-Log.txt >> shivizCollude3.log
cat 1-Log.txt >> shivizCollude3.log
cat 2-Log.txt >> shivizCollude3.log
cat 3-Log.txt >> shivizCollude3.log
cat 4-Log.txt >> shivizCollude3.log
cat 5-Log.txt >> shivizCollude3.log
cat 6-Log.txt >> shivizCollude3.log
cat 7-Log.txt >> shivizCollude3.log
echo "trial 2"
bash tearNodes.sh
bash setupNodes.sh
sleep 6
bash localhostColludePutPut.sh
sleep 1
cat regex.txt > shivizCollude2.log
cat HttpNodeLog0-Log.txt >> shivizCollude2.log
cat HttpNodeLog1-Log.txt >> shivizCollude2.log
cat 0-Log.txt >> shivizCollude2.log
cat 1-Log.txt >> shivizCollude2.log
cat 2-Log.txt >> shivizCollude2.log
cat 3-Log.txt >> shivizCollude2.log
cat 4-Log.txt >> shivizCollude2.log
cat 5-Log.txt >> shivizCollude2.log
cat 6-Log.txt >> shivizCollude2.log
cat 7-Log.txt >> shivizCollude2.log
echo "trial 1"
bash tearNodes.sh
bash setupNodes.sh
sleep 6
bash localhostColludePutPut.sh
sleep 1
cat regex.txt > shivizCollude1.log
cat HttpNodeLog0-Log.txt >> shivizCollude1.log
cat HttpNodeLog1-Log.txt >> shivizCollude1.log
cat 0-Log.txt >> shivizCollude1.log
cat 1-Log.txt >> shivizCollude1.log
cat 2-Log.txt >> shivizCollude1.log
cat 3-Log.txt >> shivizCollude1.log
cat 4-Log.txt >> shivizCollude1.log
cat 5-Log.txt >> shivizCollude1.log
cat 6-Log.txt >> shivizCollude1.log
cat 7-Log.txt >> shivizCollude1.log
