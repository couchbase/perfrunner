currentBuild.description = params.version

def testCases = [:]

def buildTests(tests) {
    def totalTests = 0
    for ( test in tests ) {
        if (
            !test.get('groups', '').equalsIgnoreCase('disabled') &&
            (params.groups == '' || test.get('groups', '').equalsIgnoreCase(params.groups))
        ) {
            if ( params.dry_run ) {
                echo test.toString()
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
            }
            totalTests++
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
                        testCases['neo'] = readJSON file: params.neo_test_suite
                    }
                    if ( params.trinity_test_suite != '' ) {
                        testCases['trinity'] = readJSON file: params.trinity_test_suite
                    }
                }
            }
        }
        stage('Weekly') {
            parallel {
                stage('AWS_Analytics') {
                    when { expression { return params.AWS_Analytics } }
                    steps {
                        buildComponent('AWS_Analytics', testCases)
                    }
                }
                stage('AWS_FTS') {
                    when { expression { return params.AWS_FTS } }
                    steps {
                        buildComponent('AWS_FTS', testCases)
                    }
                }
                stage('AWS_KV') {
                    when { expression { return params.AWS_KV } }
                    steps {
                        buildComponent('AWS_KV', testCases)
                    }
                }
                stage('AWS_N1QL') {
                    when { expression { return params.AWS_N1QL } }
                    steps {
                        buildComponent('AWS_N1QL', testCases)
                    }
                }
                stage('AWS_Rebalance') {
                    when { expression { return params.AWS_Rebalance } }
                    steps {
                        buildComponent('AWS_Rebalance', testCases)
                    }
                }
                stage('AWS_XDCR') {
                    when { expression { return params.AWS_XDCR } }
                    steps {
                        buildComponent('AWS_XDCR', testCases)
                    }
                }
                stage('AWS_Eventing') {
                    when { expression { return params.AWS_Eventing } }
                    steps {
                        buildComponent('AWS_Eventing', testCases)
                    }
                }
                stage('AWS_GSI') {
                    when { expression { return params.AWS_GSI } }
                    steps {
                        buildComponent('AWS_GSI', testCases)
                    }
                }
                stage('AWS_Tools') {
                    when { expression { return params.AWS_Tools } }
                    steps {
                        buildComponent('AWS_Tools', testCases)
                    }
                }
                stage('AWS_SGW') {
                    when { expression { return params.AWS_SGW } }
                    steps {
                        buildComponent('AWS_SGW', testCases)
                    }
                }
                stage('AWS_Infrastructure') {
                    when { expression { return params.AWS_Infrastructure } }
                    steps {
                        buildComponent('AWS_Infrastructure', testCases)
                    }
                }
                stage('Azure_KV') {
                    when { expression { return params.Azure_KV } }
                    steps {
                        buildComponent('Azure_KV', testCases)
                    }
                }
                stage('Azure_XDCR') {
                    when { expression { return params.Azure_XDCR } }
                    steps {
                        buildComponent('Azure_XDCR', testCases)
                    }
                }
                stage('Azure_N1QL') {
                    when { expression { return params.Azure_N1QL } }
                    steps {
                        buildComponent('Azure_N1QL', testCases)
                    }
                }
                stage('Azure_Rebalance') {
                    when { expression { return params.Azure_Rebalance } }
                    steps {
                        buildComponent('Azure_Rebalance', testCases)
                    }
                }
                stage('Azure_Analytics') {
                    when { expression { return params.Azure_Analytics } }
                    steps {
                        buildComponent('Azure_Analytics', testCases)
                    }
                }
                stage('Azure_FTS') {
                    when { expression { return params.Azure_FTS } }
                    steps {
                        buildComponent('Azure_FTS', testCases)
                    }
                }
                stage('Azure_Eventing') {
                    when { expression { return params.Azure_Eventing } }
                    steps {
                        buildComponent('Azure_Eventing', testCases)
                    }
                }
                stage('Azure_GSI') {
                    when { expression { return params.Azure_GSI } }
                    steps {
                        buildComponent('Azure_GSI', testCases)
                    }
                }
                stage('Azure_Tools') {
                    when { expression { return params.Azure_Tools } }
                    steps {
                        buildComponent('Azure_Tools', testCases)
                    }
                }
                stage('Azure_SGW') {
                    when { expression { return params.Azure_SGW } }
                    steps {
                        buildComponent('Azure_SGW', testCases)
                    }
                }
                stage('Azure_Infrastructure') {
                    when { expression { return params.Azure_Infrastructure } }
                    steps {
                        buildComponent('Azure_Infrastructure', testCases)
                    }
                }
                stage('GCP_FTS') {
                    when { expression { return params.GCP_FTS } }
                    steps {
                        buildComponent('GCP_FTS', testCases)
                    }
                }
                stage('GCP_KV') {
                    when { expression { return params.GCP_KV } }
                    steps {
                        buildComponent('GCP_KV', testCases)
                    }
                }
                stage('GCP_XDCR') {
                    when { expression { return params.GCP_XDCR } }
                    steps {
                        buildComponent('GCP_XDCR', testCases)
                    }
                }
                stage('GCP_N1QL') {
                    when { expression { return params.GCP_N1QL } }
                    steps {
                        buildComponent('GCP_N1QL', testCases)
                    }
                }
                stage('GCP_Analytics') {
                    when { expression { return params.GCP_Analytics } }
                    steps {
                        buildComponent('GCP_Analytics', testCases)
                    }
                }
                stage('GCP_Rebalance') {
                    when { expression { return params.GCP_Rebalance } }
                    steps {
                        buildComponent('GCP_Rebalance', testCases)
                    }
                }
                stage('GCP_Tools') {
                    when { expression { return params.GCP_Tools } }
                    steps {
                        buildComponent('GCP_Tools', testCases)
                    }
                }
                stage('GCP_Eventing') {
                    when { expression { return params.GCP_Eventing } }
                    steps {
                        buildComponent('GCP_Eventing', testCases)
                    }
                }
                stage('GCP_GSI') {
                    when { expression { return params.GCP_GSI } }
                    steps {
                        buildComponent('GCP_GSI', testCases)
                    }
                }
                stage('GCP_SGW') {
                    when { expression { return params.GCP_SGW } }
                    steps {
                        buildComponent('GCP_SGW', testCases)
                    }
                }
                stage('GCP_Infrastructure') {
                    when { expression { return params.GCP_Infrastructure } }
                    steps {
                        buildComponent('GCP_Infrastructure', testCases)
                    }
                }
            }
        }
    }
}
