pipeline {
    agent any
    parameters {
        string(name: 'SE_VERSION', defaultValue: 'v2.1.0', description: '镜像版本')
    }
    stages {
        stage('parallel stages') {
            parallel {
                stage('x86_64') {
                    agent {
                        label 'x86-10.207'
                    }
                    stages {
                        stage('clean .svn') {
                            steps {
                                sh 'find ./ -name .svn | xargs rm -rf'
                            }
                        }
                        stage('build') {
                            steps {
                                script {
                                    HARBOR_JENKINS_ID = 'a745b7c1-c866-4111-b10d-e48c7f1d0d6a'
                                    registry = 'http://192.190.50.11:8088'
                                    tag = "192.190.50.11:8088/dbs-frame/se:${env.SVN_REVISION}"
                                    image = docker.build(tag, './')
                                    docker.withRegistry(registry, HARBOR_JENKINS_ID) {
                                        image.push()
                                    }
                                }
                            }
                        }
                    }
                }
                stage('aarch64') {
                    agent {
                        label 'arm-20.56'
                    }
                    stages {
                        stage('clean .svn') {
                            steps {
                                sh 'find ./ -name .svn | xargs rm -rf'
                            }
                        }
                        stage('build') {
                            steps {
                                script {
                                    HARBOR_JENKINS_ID = 'a745b7c1-c866-4111-b10d-e48c7f1d0d6a'
                                    registry = 'http://192.190.50.11:8088'
                                    tag = "192.190.50.11:8088/dbs-arm/se:${env.SVN_REVISION}"
                                    image = docker.build(tag, './')
                                    docker.withRegistry(registry, HARBOR_JENKINS_ID) {
                                        image.push()
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
