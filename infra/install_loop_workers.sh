echo "uploading install_prereqs_worker.sh to VM IP:"

while read ip; do
  scp install_prereqs_worker.sh Crispinoadmin@"$ip":/home/Crispinoadmin/
  echo $IP
done < workers.txt
wait

#echo "Installing install_prereqs_worker.sh to VM IP:"
#
#while read ip; do
#  ssh Crispinoadmin@"$ip" "chmod +x ~/install_prereqs_worker.sh && sudo ~/install_prereqs_worker.sh" &
#  echo $IP
#done < workers.txt
#wait