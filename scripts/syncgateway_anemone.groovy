currentBuild.description = params.version

def testCases = [:]

def buildTests(tests) {
    for ( test in tests ) {
        build job: test['job'], propagate: false, parameters: [
            string(name: 'sg_build', value: params.version),
            string(name: 'cb_build', value: params.cb_build),
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
            string(name: 'cb_build', value: params.cb_build),
            string(name: 'test_type', value: test['test_type']),
            string(name: 'cluster', value: test['cluster']),
            string(name: 'load_config', value: test['load_config']),
            string(name: 'test_config', value: test['test_config']),
            string(name: 'sg_config', value: test['sg_config'])
        ]
    }
}

def buildTestsSGReplicate(tests) {
    for ( test in tests ) {
        build job: test['job'], propagate: false, parameters: [
            string(name: 'sg_build', value: params.version),
            string(name: 'cb_build', value: params.cb_build),
            string(name: 'test_config', value: test['test_config']),
            string(name: 'sg1_config', value: test['sg1_config']),
            string(name: 'sg2_config', value: test['sg2_config']),
            string(name: 'load_config1', value: test['load_config1']),
            string(name: 'load_config2', value: test['load_config2'])
        ]
    }
}

def buildTestsSGReplicateMultiCluster(tests) {
    for ( test in tests ) {
        build job: test['job'], propagate: false, parameters: [
            string(name: 'sg_build', value: params.version),
            string(name: 'cb_build', value: params.cb_build),
            string(name: 'sg_cluster_map', value: test['sg_cluster_map']),
            string(name: 'cb_cluster_map', value: test['cb_cluster_map']),
            string(name: 'sg_config', value: test['sg_config']),
            string(name: 'test', value: test['test'])
        ]
    }
}

def buildTestsSGXDCR(tests) {
    for ( test in tests ) {
        build job: test['job'], propagate: false, parameters: [
            string(name: 'sg_build', value: params.version),
            string(name: 'cb_build', value: params.cb_build),
            string(name: 'test_config', value: test['test_config']),
            string(name: 'sg1_config', value: test['sg1_config']),
            string(name: 'sg2_config', value: test['sg2_config'])
        ]
    }
}

def buildComponent(component, testCases) {
    for ( release in ['anemone'] ) {
        if ( testCases.containsKey(release) ) {
            buildTests(testCases[release][component])
        }
    }
}

def buildComponentImport(component, testCases) {
    for ( release in ['anemone'] ) {
        if ( testCases.containsKey(release) ) {
            buildTestsImport(testCases[release][component])
        }
    }
}

def buildComponentSGReplicate(component, testCases) {
    for ( release in ['anemone'] ) {
        if ( testCases.containsKey(release) ) {
            buildTestsSGReplicate(testCases[release][component])
        }
    }
}

def buildComponentSGReplicateMultiCluster(component, testCases) {
    for ( release in ['anemone'] ) {
        if ( testCases.containsKey(release) ) {
            buildTestsSGReplicateMultiCluster(testCases[release][component])
        }
    }
}

def buildComponentSGXDCR(component, testCases) {
    for ( release in ['anemone'] ) {
        if ( testCases.containsKey(release) ) {
            buildTestsSGXDCR(testCases[release][component])
        }
    }
}

pipeline {
    agent {label 'master'}
    stages {
        stage('Setup') {
            steps {
                script {
                    if ( params.anemone_test_suite != '' ) {
                        testCases['anemone'] = readJSON file: params.anemone_test_suite
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
                stage('SGReplicate') {
                    when { expression { return params.SGReplicate } }
                    steps {
                        buildComponentSGReplicate('SGReplicate', testCases)
                    }
                }
                stage('SGReplicateMulti') {
                    when { expression { return params.SGReplicateMulti } }
                    steps {
                        buildComponentSGReplicateMultiCluster('SGReplicateMulti', testCases)
                    }
                }
                stage('E2E') {
                    when { expression { return params.E2E } }
                    steps {
                        buildComponent('E2E', testCases)
                    }
                }
                stage('Resync') {
                    when { expression { return params.Resync } }
                    steps {
                        buildComponent('Resync', testCases)
                    }
                }
                stage('SGXDCR') {
                    when { expression { return params.SGXDCR } }
                    steps {
                        buildComponent('SGXDCR', testCases)
                    }
                }
                stage('LowEnd') {
                    when { expression { return params.LowEnd } }
                    steps {
                        buildComponent('LowEnd', testCases)
                    }
                }
                stage('ReadOnDemand') {
                    when { expression { return params.ReadOnDemand } }
                    steps {
                        buildComponent('ReadOnDemand', testCases)
                    }
                }
                stage('WriteOnDemand') {
                    when { expression { return params.WriteOnDemand } }
                    steps {
                        buildComponent('WriteOnDemand', testCases)
                    }
                }
                stage('SyncOnDemand') {
                    when { expression { return params.SyncOnDemand } }
                    steps {
                        buildComponent('SyncOnDemand', testCases)
                    }
                }
                stage('ReplicateOnDemand') {
                    when { expression { return params.ReplicateOnDemand } }
                    steps {
                        buildComponent('ReplicateOnDemand', testCases)
                    }
                }
                stage('ImportOnDemand') {
                    when { expression { return params.ImportOnDemand } }
                    steps {
                        buildComponentImport('ImportOnDemand', testCases)
                    }
                }
            }
        }
    }
}