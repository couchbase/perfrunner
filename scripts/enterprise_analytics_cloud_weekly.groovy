def description = ""
["AWS": params.AWS, "AZURE": params.AZURE].each { csp, enabled ->
    if ( enabled ) {
        description += "${csp}, "
    }
}
description += params.ea_version
currentBuild.description = description

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
                    string(name: 'analytics_test_config', value: test['analytics_test_config']),
                    string(name: 'kv_test_config', value: test.get('kv_test_config', '')),
                    string(name: 'cluster', value: test['cluster']),
                    string(name: 'columnar_version', value: params.ea_version),
                    string(name: 'other_version', value: params.cb_server_version),
                    string(name: 'cloud_region', value: params.region)
                ]
            }
            totalTests++
        }
    }
    echo "Total Tests ran: " + totalTests.toString()
}

def buildComponent(component, testCases) {
    for ( release in ['phoenix'] ) {
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
                    if ( params.phoenix_test_suite != '' ) {
                        testCases['phoenix'] = readJSON file: params.phoenix_test_suite
                    }
                }
            }
        }
        stage('Weekly') {
            parallel {
                stage('AWS_1') {
                    when { expression { return params.AWS } }
                    steps {
                        buildComponent('AWS_1', testCases)
                    }
                }
                stage('AWS_2') {
                    when { expression { return params.AWS } }
                    steps {
                        buildComponent('AWS_2', testCases)
                    }
                }
                stage('AWS_3') {
                    when { expression { return params.AWS } }
                    steps {
                        buildComponent('AWS_3', testCases)
                    }
                }
                stage('AZURE_1') {
                    when { expression { return params.AZURE } }
                    steps {
                        buildComponent('AZURE_1', testCases)
                    }
                }
                stage('AZURE_2') {
                    when { expression { return params.AZURE } }
                    steps {
                        buildComponent('AZURE_2', testCases)
                    }
                }
                stage('AZURE_3') {
                    when { expression { return params.AZURE } }
                    steps {
                        buildComponent('AZURE_3', testCases)
                    }
                }
            }
        }
    }
}
