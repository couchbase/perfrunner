currentBuild.description = params.version

def testCases = null

def buildTests(tests, job_name) {
    for ( test in tests ) {
        build job: job_name, propagate: false, parameters: [
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
        stage('Analytics') {
            steps {
                buildTests(testCases['Analytics'], 'triton_analytics')
            }
        }
        stage('Eventing') {
            steps {
                buildTests(testCases['Eventing'], 'triton')
            }
        }
        stage('FTS') {
            steps {
                buildTests(testCases['FTS'], 'triton')
            }
        }
        stage('GSI') {
            steps {
                buildTests(testCases['GSI'], 'triton')
            }
        }
        stage('KV') {
            steps {
                buildTests(testCases['KV'], 'triton')
            }
        }
        stage('N1QL') {
            steps {
                buildTests(testCases['N1QL'], 'triton-multiclient')
            }
        }
        stage('Tools') {
            steps {
                buildTests(testCases['Tools'], 'triton')
            }
        }
        stage('XDCR') {
            steps {
                buildTests(testCases['XDCR'], 'triton')
            }
        }
        stage('YCSB') {
            steps {
                buildTests(testCases['YCSB'], 'triton')
            }
        }
        stage('Notifications') {
            steps {
                build job: 'triton-notification', parameters: [string(name: 'version', value: params.version)]
            }
        }
    }
}
