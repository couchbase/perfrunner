currentBuild.description = params.version

def testCases = [:]

def buildTests(tests) {
    def totalTests = 0
    for ( test in tests ) {
        if ( test.containsKey("groups") && test['groups'].equalsIgnoreCase("disabled") ){
            // do nothing for disabled test
        }
        else if ( params.groups != '' && test.containsKey("groups") && test['groups'].equalsIgnoreCase(params.groups) ){
               if ( params.dry_run ){
                    echo test.toString()
                    totalTests++
               }
               else {
                    build job: test['job'], propagate: false, parameters: [
                        string(name: 'test_config', value: test['test_config']),
                        string(name: 'cluster', value: test['cluster']),
                        string(name: 'capella_env', value: params.capella_env),
                        string(name: 'cloud_backend', value: params.cloud_backend),
                        string(name: 'server_version', value: params.server_version),
                        string(name: 'version', value: params.version),
                        string(name: 'capella_server_ami', value: params.capella_server_ami),
                        string(name: 'region', value: params.region)
                    ]
                    totalTests++
               }
        }
        else if ( params.groups == '') {
               if ( params.dry_run ){
                    echo test.toString()
                    totalTests++
               }
               else {
                    build job: test['job'], propagate: false, parameters: [
                        string(name: 'test_config', value: test['test_config']),
                        string(name: 'cluster', value: test['cluster']),
                        string(name: 'capella_env', value: params.capella_env),
                        string(name: 'cloud_backend', value: params.cloud_backend),
                        string(name: 'server_version', value: params.server_version),
                        string(name: 'version', value: params.version),
                        string(name: 'capella_server_ami', value: params.capella_server_ami),
                        string(name: 'region', value: params.region)
                    ]
                    totalTests++
               }
        }
    }
    echo "Total Tests ran: " + totalTests.toString()
}

def buildComponent(component, testCases) {
    for ( release in ['trinity', 'neo'] ) {
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
                    if ( params.neo_test_suite != '' ) {
                        testCases['neo']  = readJSON file: params.neo_test_suite
                    }
                    if ( params.trinity_test_suite != '' ) {
                        testCases['trinity']  = readJSON file: params.trinity_test_suite
                    }
                }
            }
        }
        stage('Weekly') {
            parallel {
                stage('AWS') {
                    when { expression { return params.AWS } }
                    steps {
                        buildComponent('AWS', testCases)
                    }
                }
                stage('GCP') {
                    when { expression { return params.GCP } }
                    steps {
                        buildComponent('GCP', testCases)
                    }
                }
                stage('Azure') {
                    when { expression { return params.Azure } }
                    steps {
                        buildComponent('Azure', testCases)
                    }
                }
            }
        }
    }
}
