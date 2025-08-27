kubectl create namespace airflow

kubectl create secret generic airflow-metadata --from-literal=connection="" -n airflow

kubectl create secret generic airflow-broker-url --from-literal=connection="" -n airflow

kubectl create secret generic airflow-fernet-key --from-literal=fernet-key="" -n airflow

kubectl create secret generic airflow-webserver-secret-key --from-literal=webserver-secret-key="" -n airflow

kubectl create secret generic airflow-git-ssh --from-file=gitSshKey=git-sync-ssh -n airflow

kubectl create secret generic airflow-service-account-secret --from-file=service-account.json=google-service-account.json -n airflow

kubectl create secret generic airflow-task-env --from-env-file=.env -n airflow
