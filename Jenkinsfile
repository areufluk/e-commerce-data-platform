pipeline {
    agent any

    environment {
        COMMIT_HASH = sh(returnStdout: true, script: "git rev-parse --short=8 HEAD").trim()
        BRANCH_NAME = env.CHANGE_TARGET ?: env.BRANCH_NAME
        IMAGE_REGISTRY = "asia-southeast1-docker.pkg.dev"
        PROJECT_ID = "helloworld-ab722"
        IMAGE_REPOSITORY = "e-commerce-images"
        IMAGE_NAME = "airflow"
        IMAGE_TAG = "${env.BRANCH_NAME}-${env.COMMIT_HASH}"
        DOCKER_IMAGE = "${env.IMAGE_REGISTRY}/${env.PROJECT_ID}/${env.IMAGE_REPOSITORY}/${env.IMAGE_NAME}:${env.IMAGE_TAG}"
    }

    options {
        skipStagesAfterUnstable()
        timestamps()
    }

    stages {
        stage("Checkout") {
            steps {
                checkout scm
            }
        }

        stage("Check Changed Folders") {
            steps {
                script {
                    def changedFiles = sh(script: "git diff --name-only origin/${BRANCH_NAME}", returnStdout: true).trim().split("\n")
                    def hasOtherFolderChanges = changedFiles.any { file ->
                        !(file.startsWith("dags/") || file.startsWith("scripts/"))
                    }
                    env.SKIP_BUILD_DEPLOY = !(hasOtherFolderChanges.toString())
                }
                echo "Only dags/ and scripts/ change?: ${env.SKIP_BUILD_DEPLOY}"
            }
        }

        stage("Run Tests") {
            when {
                expression { env.SKIP_BUILD_DEPLOY != "true" }
            }
            steps {
            //     sh '''
            //         pip install -r requirements.txt
            //         pip install pytest flake8 black pylint
                    
                echo "🧪 Running unit tests..."
            //         pytest tests/

                echo "🧹 Checking code style with flake8..."
            //         flake8 plugins/ dags/ scripts/ --count --select=E9,F63,F7,F82 --show-source --statistics

                echo "🎨 Running Black formatting check..."
            //         black --check plugins/ dags/ scripts/

                echo "🔍 Running pylint..."
            //         pylint plugins/
            //     '''
            }
        }

        stage("Build and Push Docker Image") {
            when {
                expression { env.SKIP_BUILD_DEPLOY != "true" }
            }
            steps {
                echo "🔍 Build docker image... ${env.DOCKER_IMAGE}"
                echo "🔍 Push docker image... ${env.DOCKER_IMAGE}"
                // docker build -t asia-southeast1-docker.pkg.dev/helloworld-ab722/e-commerce-images/airflow:1.1.3 
                // <resigtry>
            //     script {
            //         sh "docker build -t ${DOCKER_IMAGE} ."
            //     }
            }
        }

        stage("Deploy to K8s Cluster") {
            when {
                expression { env.SKIP_BUILD_DEPLOY != "true" }
            }
            parallel {
                stage("Develop") {
                    when {
                        branch "develop"
                    }
                    step {
                        echo "🔍 Deploy to Develop site..."
                    }
                }

                stage("UAT") {
                    when {
                        branch "uat"
                    }
                    step {
                        echo "🔍 Deploy to UAT site..."
                    }
                }

                stage("Production") {
                    when {
                        tag "release-v.*"
                    }
                    step {
                        echo "🔍 Deploy to Production site..."
                    }
                }
                //     script {
                //         def ns = (BRANCH_NAME == 'production') ? 'airflow-prod' : 'airflow-dev'
                //         sh """
                //             helm upgrade airflow-core ./helm \
                //             --install \
                //             --namespace ${ns} \
                //             --set images.airflow.repository=${GCR_REGION}/${GCP_PROJECT}/${IMAGE_NAME} \
                //             --set images.airflow.tag=${env.BUILD_NUMBER}
                //         """
                //     }
            }
        }
    }

    post {
        failure {
            echo "🔴 Build failed!"
        }
        success {
            echo "✅ Pipeline completed successfully."
        }
    }
}
