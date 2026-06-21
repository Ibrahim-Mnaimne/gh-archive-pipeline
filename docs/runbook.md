# Airflow EC2 Runbook

## Daily Startup

PowerShell window 1 - shell session:

    aws ssm start-session --target <your-instance-id> --region eu-north-1
    sudo su - ec2-user
    cd ~/airflow-project
    sudo systemctl start docker.socket
    sudo systemctl start docker
    docker compose up -d
    docker compose ps

PowerShell window 2 - UI port-forward (optional):

    aws ssm start-session --target <your-instance-id> --region eu-north-1 --document-name AWS-StartPortForwardingSession --parameters "portNumber=8080,localPortNumber=8080"

Then browse http://localhost:8080. Keep window 2 open while using UI.

## Daily Shutdown

    docker compose down
    aws ec2 stop-instances --region eu-north-1 --instance-ids <your-instance-id>
    aws rds stop-db-instance --region eu-north-1 --db-instance-identifier airflow-rds

Note: AWS auto-starts stopped RDS instances after 7 days.

## Troubleshooting

SSM session hangs: RAM starvation. Stop EC2, wait, restart.
Wrong directory error: cd ~/airflow-project
Cannot connect to Docker daemon: sudo systemctl start docker.socket && sudo systemctl start docker
DAG import error: check docker compose logs airflow-scheduler --tail=30
RDS connection timeout: Start RDS, verify security group rule
404 from GH Archive: File not published yet. Wait 5-10 min, retry task.
