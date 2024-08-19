currentBuild.description = params.version

def testCases = [:]

def buildTests(tests) {
    def totalTests = 0
    for ( test in tests ) {
        if (
            !test.get('groups', '').equalsIgnoreCase('disabled') &&
            (params.groups == '' || test.get('groups', '').equalsIgnoreCase(params.groups))
        ) {
            if ( params.dry_run ){
                echo test.toString()
            }
            else {
                build job: test['job'], propagate: false, parameters: [
                    string(name: 'test_config', value: test['test_config']),
                    string(name: 'cluster', value: test['cluster']),
                    string(name: 'capella_env', value: params.capella_env),
                    string(name: 'cloud_backend', value: params.cloud_backend),
                    string(name: 'operational_server_version', value: params.operational_server_version),
                    string(name: 'columnar_ami', value: params.columnar_ami),
                    string(name: 'region', value: params.region)
                ]
            }
            totalTests++
        }
    }
    echo "Total Tests ran: " + totalTests.toString()
}

def buildComponent(component, testCases) {
    for ( release in ['goldfish'] ) {
        if ( testCases.containsKey(release) ) {
            echo "building tests for " + release + " : " + component
            buildTests(testCases[release][component])
        }
    }
}

pipeline {
    agent {label 'master'}
    stages {
        stage('Setup') {
            steps {
                script {
                    if ( params.goldfish_test_suite != '' ) {
                        testCases['goldfish'] = readJSON file: params.goldfish_test_suite
                    }
                }
            }
        }
        stage('Weekly') {
            parallel {
                stage('AWS_CH2') {
                    when { expression { return params.AWS_CH2 } }
                    steps {
                        buildComponent('AWS_CH2', testCases)
                    }
                }
            }
        }
    }
}
