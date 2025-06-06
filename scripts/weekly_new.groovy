import groovy.time.TimeCategory
import groovy.time.TimeDuration

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
                    string(name: 'version', value: params.version)
                ]
            }
            totalTests++
        }
    }
    echo "Total Tests ran: " + totalTests.toString()
}

def buildComponent(component, testCases) {
    for ( release in ['morpheus', 'trinity', 'neo', 'cheshire-cat', 'mad-hatter', 'alice', 'vulcan', 'spock', 'watson'] ) {
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
                    if ( params.neo_test_suite != '' ) {
                        testCases['neo']  = readJSON file: params.neo_test_suite
                    }
                    if ( params.trinity_test_suite != '' ) {
                        testCases['trinity']  = readJSON file: params.trinity_test_suite
                    }
                    if ( params.morpheus_test_suite != '' ) {
                        testCases['morpheus']  = readJSON file: params.morpheus_test_suite
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
                stage("FTS-Rebalance"){
                    when { expression { return params.FTS_Rebalance } }
                    steps {
                        buildComponent('FTS-Rebalance', testCases)
                    }
                }
                stage("FTS-N1FTY") {
                    when { expression { return params.FTS_N1FTY } }
                    steps {
                        buildComponent('FTS-N1FTY', testCases)
                    }
                }
                stage("FTS-MultiIndex"){
                    when { expression { return params.FTS_MultiIndex } }
                    steps {
                        buildComponent('FTS-MultiIndex', testCases)
                    }
                }
                stage('GSI') {
                    when { expression { return params.GSI } }
                    steps {
                        buildComponent('GSI', testCases)
                    }
                }
                stage('GSI-DGM') {
                    when { expression { return params.GSI_DGM } }
                    steps {
                        buildComponent('GSI-DGM', testCases)
                    }
                }
                stage('GSI-Hemera') {
                    when { expression { return params.GSI_Hemera } }
                    steps {
                        buildComponent('GSI-Hemera', testCases)
                    }
                }
                stage('GSI-ForestDB') {
                    when { expression { return params.GSI_ForestDB } }
                    steps {
                        buildComponent('GSI-ForestDB', testCases)
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
                    when { expression { return params.KV_Hercules } }
                    steps {
                        buildComponent('KV-Hercules', testCases)
                    }
                }
                stage('KV-Athena') {
                    when { expression { return params.KV_Athena } }
                    steps {
                        buildComponent('KV-Athena', testCases)
                    }
                }
                stage('KV-Windows') {
                    when { expression { return params.KV_Windows } }
                    steps {
                        buildComponent('KV-Windows', testCases)
                    }
                }
                stage('KV-DGM') {
                    when { expression { return params.KV_DGM } }
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
                stage('CDC') {
                    when { expression {return params.KV_CDC } }
                    steps {
                        buildComponent('CDC', testCases)
                    }
                }
                stage('N1QL') {
                    when { expression { return params.N1QL } }
                    steps {
                        buildComponent('N1QL', testCases)
                    }
                }
                stage('N1QL-SQL') {
                    when { expression { return params.N1QL } }
                    steps {
                        buildComponent('N1QL-SQL', testCases)
                    }
                }
                stage('N1QL-Windows') {
                    when { expression { return params.N1QL_Windows } }
                    steps {
                        buildComponent('N1QL-Windows', testCases)
                    }
                }
                stage('N1QL-Arke') {
                    when { expression { return params.N1QL_Arke } }
                    steps {
                        buildComponent('N1QL-Arke', testCases)
                    }
                }
                stage('N1QL-PYTPCC') {
                    when { expression { return params.N1QL_PYTPCC } }
                    steps {
                        buildComponent('N1QL-PYTPCC', testCases)
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
                stage('Rebalance-C1') {
                    when { expression { return params.Rebalance } }
                    steps {
                        buildComponent('Rebalance-C1', testCases)
                    }
                }
                stage('Rebalance-C2') {
                    when { expression { return params.Rebalance } }
                    steps {
                        buildComponent('Rebalance-C2', testCases)
                    }
                }
                stage('Rebalance-Large-Scale') {
                    when { expression { return params.Rebalance } }
                    steps {
                        buildComponent('Rebalance-Large-Scale', testCases)
                    }
                }
                stage('Rebalance-Large-Scale-C1') {
                    when { expression { return params.Rebalance } }
                    steps {
                        buildComponent('Rebalance-Large-Scale-C1', testCases)
                    }
                }
                stage('Rebalance-Large-Scale-C2') {
                    when { expression { return params.Rebalance } }
                    steps {
                        buildComponent('Rebalance-Large-Scale-C2', testCases)
                    }
                }
                stage('Rebalance-Demeter') {
                    when { expression { return params.Rebalance_Demeter } }
                    steps {
                        buildComponent('Rebalance-Demeter', testCases)
                    }
                }
                stage('Rebalance-Windows') {
                    when { expression { return params.Rebalance_Windows } }
                    steps {
                        buildComponent('Rebalance-Windows', testCases)
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
                stage('XDCR-BIDI') {
                    when { expression { return params.XDCR } }
                    steps {
                        buildComponent('XDCR-BIDI', testCases)
                    }
                }
                stage('XDCR-C1') {
                    when { expression { return params.XDCR } }
                    steps {
                        buildComponent('XDCR-C1', testCases)
                    }
                }
                stage('XDCR-C2') {
                    when { expression { return params.XDCR } }
                    steps {
                        buildComponent('XDCR-C2', testCases)
                    }
                }
                stage('XDCR-Windows') {
                    when { expression { return params.XDCR_Windows } }
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
                stage('YCSB-Hebe') {
                    when { expression { return params.YCSB } }
                    steps {
                        buildComponent('YCSB-Hebe', testCases)
                    }
                }
                stage('Magma-Legacy') {
                    when { expression { return params.MagmaLegacy } }
                    steps {
                        buildComponent('Magma-Legacy', testCases)
                    }
                }
                stage('Magma1') {
                    when { expression { return params.Magma1 } }
                    steps {
                        buildComponent('Magma1', testCases)
                    }
                }
                stage('Magma2') {
                    when { expression { return params.Magma2 } }
                    steps {
                        buildComponent('Magma2', testCases)
                    }
                }
                stage('MagmaNVME') {
                    when { expression { return params.MagmaNVME } }
                    steps {
                        buildComponent('MagmaNVME', testCases)
                    }
                }
                stage('GSI-Recovery') {
                    when { expression { return params.GSI_Recovery } }
                    steps {
                        buildComponent('GSI-Recovery', testCases)
                    }
                }
                stage('Cloud') {
                    when { expression { return params.Cloud } }
                    steps {
                        buildComponent('Cloud', testCases)
                    }
                }
                stage('GSI-Ephemeral') {
                    when { expression { return params.GSI_Ephemeral } }
                    steps {
                        buildComponent('GSI-Ephemeral', testCases)
                    }
                }
                stage('GSI-Compression') {
                    when { expression { return params.GSI_Compression } }
                    steps {
                        buildComponent('GSI-Compression', testCases)
                    }
                }
                stage('GSI-DDl') {
                    when { expression { return params.GSI_DDl } }
                    steps {
                        buildComponent('GSI-DDl', testCases)
                    }
                }
                stage('GSI-Rebalance') {
                    when { expression { return params.GSI_Rebalance } }
                    steps {
                        buildComponent('GSI-Rebalance', testCases)
                    }
                }
            }
        }
    }
}
