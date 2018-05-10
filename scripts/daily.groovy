currentBuild.description = params.version

def testCases = null

def buildTests(tests) {
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
                buildTests(testCases['Eventing'])
            }
        }
        stage('FTS') {
            steps {
                buildTests(testCases['FTS'])
            }
        }
        stage('GSI') {
            steps {
                buildTests(testCases['GSI'])
            }
        }
        stage('KV') {
            steps {
                buildTests(testCases['KV'])
            }
        }
        stage('N1QL') {
            steps {
                buildTests(testCases['N1QL'])
            }
        }
        stage('Tools') {
            steps {
                buildTests(testCases['Tools'])
            }
        }
        stage('XDCR') {
            steps {
                buildTests(testCases['XDCR'])
            }
        }
        stage('YCSB') {
            steps {
                buildTests(testCases['YCSB'])
            }
        }
        stage('Notifications') {
            steps {
                build job: 'triton-notification', parameters: [string(name: 'version', value: params.version)]
            }
        }
    }
}
