

## Steps
1. Create 2 EC2 instances: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EC2_GetStarted.html;
2. Connect to the instances created using SSH:
<br>
```shell script
chmod 400 key-pair-name.pem
ssh -i /path/key-pair-name.pem instance-user-name@instance-public-dns-name
```
key-pair.pem is the file downloaded when created the instance.
<br>
instance-user-name is default to "ec2-user"
<br>
instance-public-dns-name is the "Public IPv4 DNS" filed in the instance console dashboard