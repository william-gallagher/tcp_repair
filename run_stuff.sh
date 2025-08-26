gcc -g -o send send.c
gcc -g -o receive receive.c
sudo setcap 'cap_net_admin=ep' send 

rm destination.txt
rm /tmp/lock_file
./send &
./send &
./receive


cmp source.txt destination.txt
