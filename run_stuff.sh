gcc -g -o send -pthread send.c
gcc -g -o receive -pthread receive.c
sudo setcap 'cap_net_admin=ep' send 

rm destination.txt
rm /tmp/lock_file
./send &
./send &
./receive


cmp source.txt destination.txt
