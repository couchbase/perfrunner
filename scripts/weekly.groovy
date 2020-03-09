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
    for ( release in ['watson', 'spock', 'vulcan', 'alice', 'mad-hatter', 'cheshire-cat'] ) {
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
                    if ( params.alice_test_suite != '' ) {
                        testCases['alice']  = readJSON file: params.alice_test_suite
                    }
                    if ( params.madhatter_test_suite != '' ) {
                        testCases['mad-hatter']  = readJSON file: params.madhatter_test_suite
                    }
                    if ( params.cheshire_cat_test_suite != '' ) {
                        testCases['cheshire-cat']  = readJSON file: params.cheshire_cat_test_suite
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
                stage('Eventing') {
                    when { expression { return params.Eventing } }
                    steps {
                        buildComponent('Eventing', testCases)
                    }
                }
                stage('FTS') {
                    when { expression { return params.FTS } }
                    steps {
                        buildComponent('FTS', testCases)
                    }
                }
                stage('GSI') {
                    when { expression { return params.GSI } }
                    steps {
                        buildComponent('GSI', testCases)
                    }
                }
                stage('KV') {
                    when { expression { return params.KV } }
                    steps {
                        buildComponent('KV', testCases)
                        buildComponent('DCP', testCases)
                    }
                }
                stage('KV-Hercules') {
                    when { expression { return params.Hercules } }
                    steps {
                        buildComponent('KV-Hercules', testCases)
                    }
                }
                stage('KV-Windows') {
                    when { expression { return params.KV } }
                    steps {
                        buildComponent('KV-Windows', testCases)
                    }
                }
                stage('KV-DGM') {
                    when { expression { return params.KV } }
                    steps {
                        buildComponent('KV-DGM', testCases)
                    }
                }
                stage('KV-SSL') {
                    when { expression { return params.SSL } }
                    steps {
                        buildComponent('KV-SSL', testCases)
                    }
                }
                stage('N1QL') {
                    when { expression { return params.N1QL } }
                    steps {
                        buildComponent('N1QL', testCases)
                    }
                }
                stage('N1QL-Windows') {
                    when { expression { return params.N1QL } }
                    steps {
                        buildComponent('N1QL-Windows', testCases)
                    }
                }
                stage('N1QL-Arke') {
                    when { expression { return params.N1QL } }
                    steps {
                        buildComponent('N1QL-Arke', testCases)
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
                stage('XDCR-Windows') {
                    when { expression { return params.XDCR } }
                    steps {
                        buildComponent('XDCR-Windows', testCases)
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
