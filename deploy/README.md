Once you have created a GCE project, add your public key through the developer's console (Compute --> Compute Engine --> Metadata --> SSH Keys)
Generate a private key and download it to your local machine (APIs & auth --> Credentials) 
Convert the .p12 key using the following command:
	openssl pkcs12 -in /path/to/pkey.p12 -passin pass:notasecret -nodes -nocerts | openssl rsa -out pkey.pem
Fill in gce.ini with your service account email address (found on the same page as the private key download), and include the path to the converted .pem key and your project ID
Apply the patches included (ansible-mesos.patch and ansible-docker.patch) to the respective roles directories
Modify the "remote_user" line in ansible.cfg with your gmail username, in the form username_gmail_com

GCE Deploy
ansible-playbook -i localhost_inventory --private-key ~/.ssh/pkey.pem gce_create_nodes.yml
ansible-playbook -vvvv -i gce.py --private-key ~/.ssh/pkey.pem gce_install_software.yml

You can now open the SSH terminal for mesos-master (Compute --> Compute Engine --> VM Instances) and dispatch jobs to the slave VMs with commands like the following:
	mesos-execute --command="mkdir /home/clalansingh_gmail_com/lol" --master=mesos-master:5050 --name=hi
