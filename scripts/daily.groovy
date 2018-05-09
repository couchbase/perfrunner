currentBuild.description = params.version

def testCases = null

def build_all(tests) {
    for ( test in tests ) {
        build job: 'triton', propagate: false, parameters: [
            string(name: 'test_config', value: test['test_config']),
            string(name: 'cluster', value: test['cluster']),
            string(name: 'version', value: params.version)
        ]
    }
}

pipeline {
    agent {label 'master'}
    stages {
        stage('Setup') {
            steps {
                script {
                    testCases = readJSON file: params.test_suite
                }
            }
        }
        stage('Eventing') {
            steps {
                build_all(testCases['Eventing'])
            }
        }
        stage('FTS') {
            steps {
                build_all(testCases['FTS'])
            }
        }
        stage('GSI') {
            steps {
                build_all(testCases['GSI'])
            }
        }
        stage('KV') {
            steps {
                build_all(testCases['KV'])
            }
        }
        stage('N1QL') {
            steps {
                build_all(testCases['N1QL'])
            }
        }
        stage('Tools') {
            steps {
                build_all(testCases['Tools'])
            }
        }
        stage('XDCR') {
            steps {
                build_all(testCases['XDCR'])
            }
        }
        stage('YCSB') {
            steps {
                build_all(testCases['YCSB'])
            }
        }
        stage('Notifications') {
            steps {
                build job: 'triton-notification', parameters: [string(name: 'version', value: params.version)]
            }
        }
    }
}
