

## Steps
1. Create 2 identical EC2 instances: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EC2_GetStarted.html;
2. Connect to the instances created using SSH;
3. Deploy the flask app in the 2 instances based on gunicore3;
4. Launch the AWS load balancer, which will distribute HTTP requests between the two instances;
5. Set the security group (inbound and outbound traffic)
