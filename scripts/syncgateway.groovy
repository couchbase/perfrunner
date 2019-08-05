currentBuild.description = params.version

def testCases = null

def buildTests(tests, job_name) {
    for ( test in tests ) {
        build job: job_name, propagate: false, parameters: [
            string(name: 'sg_build', value: params.version),
            string(name: 'cb_build', value: test['cb_build']),
            string(name: 'sg_cluster_map', value: test['sg_cluster_map']),
            string(name: 'cb_cluster_map', value: test['cb_cluster_map']),
            string(name: 'sg_config', value: test['sg_config']),
            string(name: 'test', value: test['test'])
        ]
    }
}

def buildTestsImport(tests, job_name) {
    for ( test in tests ) {
        build job: job_name, propagate: false, parameters: [
            string(name: 'sg_build', value: params.version),
            string(name: 'cb_build', value: test['cb_build']),
            string(name: 'test_type', value: test['test_type']),
            string(name: 'cluster', value: test['cluster']),
            string(name: 'load_config', value: test['load_config']),
            string(name: 'test_config', value: test['test_config'])
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
        stage('Read') {
            steps {
                buildTests(testCases['Read'], 'syncgteway-hebe-new')
            }
        }
        stage('Write') {
            steps {
                buildTests(testCases['Write'], 'syncgteway-hebe-new')
            }
        }
        stage('Sync') {
            steps {
                buildTests(testCases['Sync'], 'syncgteway-hebe-new')
            }
        }
        stage('Query') {
            steps {
                buildTests(testCases['Query'], 'syncgteway-hebe-new')
            }
        }
        stage('Import') {
            steps {
                buildTestsImport(testCases['Import'], 'hebe_sg_import')
            }
        }
        stage('Replicate') {
            steps {
                buildTests(testCases['Replicate'], 'syncgteway-hebe')
            }
        }
    }
}
