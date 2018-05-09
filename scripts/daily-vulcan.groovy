currentBuild.description = params.version

def testCases = [
    Eventing: [
        [cluster: 'triton_eventing.spec', test_config: 'eventing/eventing_throughput100M_fun_bktop.test'],
        [cluster: 'triton_eventing.spec', test_config: 'eventing/eventing_throughput100M_timer_bktop.test']
    ],
    FTS: [
        [cluster: 'triton_fts.spec', test_config: 'fts/fts_thr_wiki_med.test'],
        [cluster: 'triton_fts.spec', test_config: 'fts/fts_thr_wiki_and_med_or_hi_hi.test']
    ],
    GSI: [
        [cluster: 'triton.spec', test_config: 'gsi/gsi_200M_moi.test'],
        [cluster: 'triton.spec', test_config: 'gsi/gsi_200M_plasma.test']
    ],
    KV: [
        [cluster: 'triton_kv.spec', test_config: 'kv/dcp_max_throughput_250M.test'],
        [cluster: 'triton_kv.spec', test_config: 'kv/kv_max_ops_mixed.test'],
        [cluster: 'triton_kv.spec', test_config: 'kv/kv_max_ops_reads.test'],
        [cluster: 'triton_kv.spec', test_config: 'kv/kv_max_ops_writes.test'],
        [cluster: 'triton_kv.spec', test_config: 'kv/kv_max_ops_mixed_ephemeral.test'],
        [cluster: 'triton_kv.spec', test_config: 'kv/reb_in_100M.test']
    ],
    N1QL: [
        [cluster: 'triton.spec', test_config: 'n1ql/n1ql_thr_Q1_5M.test'],
        [cluster: 'triton.spec', test_config: 'n1ql/n1ql_thr_Q2_5M_moi_false.test'],
        [cluster: 'triton.spec', test_config: 'n1ql/n1ql_thr_Q2_5M_moi_ok.test'],
        [cluster: 'triton.spec', test_config: 'n1ql/n1ql_thr_Q2_5M_plasma_false.test'],
        [cluster: 'triton.spec', test_config: 'n1ql/n1ql_thr_Q2_5M_plasma_ok.test']
    ],
    Tools: [
        [cluster: 'triton_kv.spec', test_config: 'tools/backup_100M.test']
    ],
    XDCR: [
        [cluster: 'triton_2x2.spec', test_config: 'xdcr/xdcr_init_1x1_unidir_25M.test'],
        [cluster: 'triton_2x2.spec', test_config: 'xdcr/xdcr_init_1x1_unidir_25M_opt.test']
    ],
    YCSB: [
        [cluster: 'triton_ycsb.spec', test_config: 'ycsb/ycsb_workloada_10M.test'],
        [cluster: 'triton_ycsb.spec', test_config: 'ycsb/ycsb_workloada_10M_ephemeral.test'],
        [cluster: 'triton_kv.spec',   test_config: 'ycsb/ycsb_workloadd_100M.test'],
        [cluster: 'triton_ycsb.spec', test_config: 'ycsb/ycsb_workloade_10M_moi.test']
    ]
]

def build_all(tests) {
    for ( test in tests ) {
        build job: 'triton', propagate: false, parameters: [
            string(name: 'test_config', value: test['test_config']),
            string(name: 'cluster', value: test['cluster']),
            string(name: 'version', value: params.version)
        ]
    }
}

pipeline {
    agent {label 'master'}
    stages {
        stage('Eventing') {
            steps {
                build_all(testCases['Eventing'])
            }
        }
        stage('FTS') {
            steps {
                build_all(testCases['FTS'])
            }
        }
        stage('GSI') {
            steps {
                build_all(testCases['GSI'])
            }
        }
        stage('KV') {
            steps {
                build_all(testCases['KV'])
            }
        }
        stage('N1QL') {
            steps {
                build_all(testCases['N1QL'])
            }
        }

        stage('Tools') {
            steps {
                build_all(testCases['Tools'])
            }
        }

        stage('XDCR') {
            steps {
                build_all(testCases['XDCR'])
            }
        }

        stage('YCSB') {
            steps {
                build_all(testCases['YCSB'])
            }
        }

        stage('Notifications') {
            steps {
                build job: 'triton-notification', parameters: [string(name: 'version', value: params.version)]
            }
        }
    }
}
