currentBuild.description = params.version

def testCases = [:]

def buildTests(tests) {
    for ( test in tests ) {
        build job: test['job'], propagate: false, parameters: [
            string(name: 'test_config', value: test['test_config']),
            string(name: 'cluster', value: test['cluster']),
            string(name: 'version', value: params.version)
        ]
    }
}

def buildComponent(component, testCases) {
    for ( release in ['watson', 'spock', 'vulcan'] ) {
        if ( testCases.containsKey(release) ) {
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
                    if ( params.watson_test_suite != '' ) {
                        testCases['watson'] = readJSON file: params.watson_test_suite
                    }
                    if ( params.spock_test_suite != '' ) {
                        testCases['spock']  = readJSON file: params.spock_test_suite
                    }
                    if ( params.vulcan_test_suite != '' ) {
                        testCases['vulcan']  = readJSON file: params.vulcan_test_suite
                    }
                }
            }
        }
        stage('Weekly') {
            parallel {
                stage('Analytics') {
                    when { expression { return params.Analytics } }
                    steps {
                        buildComponent('Analytics', testCases)
                    }
                }
                stage('KV') {
                    when { expression { return params.KV } }
                    steps {
                        buildComponent('KV', testCases)
                        buildComponent('DCP', testCases)
                    }
                }
                stage('KV-DGM') {
                    when { expression { return params.KV } }
                    steps {
                        buildComponent('KV-DGM', testCases)
                    }
                }
                stage('N1QL') {
                    when { expression { return params.N1QL } }
                    steps {
                        buildComponent('N1QL', testCases)
                    }
                }
                stage('Tools') {
                    when { expression { return params.Tools } }
                    steps {
                        buildComponent('Tools', testCases)
                    }
                }
                stage('Rebalance') {
                    when { expression { return params.Rebalance } }
                    steps {
                        buildComponent('Rebalance', testCases)
                    }
                }
                stage('Rebalance-Large-Scale') {
                    when { expression { return params.Rebalance } }
                    steps {
                        buildComponent('Rebalance-Large-Scale', testCases)
                    }
                }
                stage('Views') {
                    when { expression { return params.Views } }
                    steps {
                        buildComponent('Views', testCases)
                    }
                }
                stage('XDCR') {
                    when { expression { return params.XDCR } }
                    steps {
                        buildComponent('XDCR', testCases)
                    }
                }
                stage('YCSB') {
                    when { expression { return params.YCSB } }
                    steps {
                        buildComponent('YCSB', testCases)
                    }
                }
            }
        }
    }
}
