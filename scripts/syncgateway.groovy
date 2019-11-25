currentBuild.description = params.version

def testCases = [:]

def buildTests(tests) {
    for ( test in tests ) {
        build job: test['job'], propagate: false, parameters: [
            string(name: 'sg_build', value: params.version),
            string(name: 'cb_build', value: test['cb_build']),
            string(name: 'sg_cluster_map', value: test['sg_cluster_map']),
            string(name: 'cb_cluster_map', value: test['cb_cluster_map']),
            string(name: 'sg_config', value: test['sg_config']),
            string(name: 'test', value: test['test'])
        ]
    }
}

def buildTestsImport(tests) {
    for ( test in tests ) {
        build job: test['job'], propagate: false, parameters: [
            string(name: 'sg_build', value: params.version),
            string(name: 'cb_build', value: test['cb_build']),
            string(name: 'test_type', value: test['test_type']),
            string(name: 'cluster', value: test['cluster']),
            string(name: 'load_config', value: test['load_config']),
            string(name: 'test_config', value: test['test_config'])
        ]
    }
}


def buildComponent(component, testCases) {
    for ( release in ['cobalt', 'mercury'] ) {
        if ( testCases.containsKey(release) ) {
            buildTests(testCases[release][component])
        }
    }
}

def buildComponentImport(component, testCases) {
    for ( release in ['cobalt', 'mercury'] ) {
        if ( testCases.containsKey(release) ) {
            buildTestsImport(testCases[release][component])
        }
    }
}


pipeline {
    agent {label 'master'}
    stages {
        stage('Setup') {
            steps {
                script {
                    if ( params.cobalt_test_suite != '' ) {
                        testCases['cobalt'] = readJSON file: params.cobalt_test_suite
                    }
                    if ( params.mercury_test_suite != '' ) {
                        testCases['mercury']  = readJSON file: params.mercury_test_suite
                    }
                }
            }
        }
        stage('Weekly') {
            parallel {
                stage('Read') {
                    when { expression { return params.Read } }
                    steps {
                        buildComponent('Read', testCases)
                    }
                }
                stage('Write') {
                    when { expression { return params.Write } }
                    steps {
                        buildComponent('Write', testCases)
                    }
                }
                stage('Sync') {
                    when { expression { return params.Sync } }
                    steps {
                        buildComponent('Sync', testCases)
                    }
                }
                stage('Query') {
                    when { expression { return params.Query } }
                    steps {
                        buildComponent('Query', testCases)
                    }
                }
                stage('Replicate') {
                    when { expression { return params.Replicate } }
                    steps {
                        buildComponent('Replicate', testCases)
                    }
                }
                stage('Import') {
                    when { expression { return params.Import } }
                    steps {
                        buildComponentImport('Import', testCases)
                    }
                }
            }
        }
    }
}