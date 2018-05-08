currentBuild.description = params.version

pipeline {
    agent {label 'master'}
    stages {
        stage('Eventing') {
            steps {
                build job: 'triton', propagate: false, parameters: [
                    string(name: 'test_config', value: 'eventing/eventing_throughput100M_fun_bktop.test'),
                    string(name: 'cluster', value: 'triton_eventing.spec'),
                    string(name: 'version', value: params.version)
                ]
                build job: 'triton', propagate: false, parameters: [
                    string(name: 'test_config', value: 'eventing/eventing_throughput100M_timer_bktop.test'),
                    string(name: 'cluster', value: 'triton_eventing.spec'),
                    string(name: 'version', value: params.version)
                ]
            }
        }

        stage('FTS') {
            steps {
                build job: 'triton', propagate: false, parameters: [
                    string(name: 'test_config', value: 'fts/fts_thr_wiki_med.test'),
                    string(name: 'cluster', value: 'triton_fts.spec'),
                    string(name: 'version', value: params.version)
                ]
                build job: 'triton', propagate: false, parameters: [
                    string(name: 'test_config', value: 'fts/fts_thr_wiki_and_med_or_hi_hi.test'),
                    string(name: 'cluster', value: 'triton_fts.spec'),
                    string(name: 'version', value: params.version)
                ]
            }
        }

        stage('GSI') {
            steps {
                build job: 'triton', propagate: false, parameters: [
                    string(name: 'test_config', value: 'gsi/gsi_200M_moi.test'),
                    string(name: 'cluster', value: 'triton.spec'),
                    string(name: 'version', value: params.version)
                ]
                build job: 'triton', propagate: false, parameters: [
                    string(name: 'test_config', value: 'gsi/gsi_200M_plasma.test'),
                    string(name: 'cluster', value: 'triton.spec'),
                    string(name: 'version', value: params.version)
                ]
            }
        }

        stage('KV') {
            steps {
                build job: 'triton', propagate: false, parameters: [
                    string(name: 'test_config', value: 'kv/dcp_max_throughput_250M.test'),
                    string(name: 'cluster', value: 'triton_kv.spec'),
                    string(name: 'version', value: params.version)
                ]
                build job: 'triton', propagate: false, parameters: [
                    string(name: 'test_config', value: 'kv/kv_max_ops_mixed.test'),
                    string(name: 'cluster', value: 'triton_kv.spec'),
                    string(name: 'version', value: params.version)
                ]
                build job: 'triton', propagate: false, parameters: [
                    string(name: 'test_config', value: 'kv/kv_max_ops_reads.test'),
                    string(name: 'cluster', value: 'triton_kv.spec'),
                    string(name: 'version', value: params.version)
                ]
                build job: 'triton', propagate: false, parameters: [
                    string(name: 'test_config', value: 'kv/kv_max_ops_writes.test'),
                    string(name: 'cluster', value: 'triton_kv.spec'),
                    string(name: 'version', value: params.version)
                ]
                build job: 'triton', propagate: false, parameters: [
                    string(name: 'test_config', value: 'kv/kv_max_ops_mixed_ephemeral.test'),
                    string(name: 'cluster', value: 'triton_kv.spec'),
                    string(name: 'version', value: params.version)
                ]
                build job: 'triton', propagate: false, parameters: [
                    string(name: 'test_config', value: 'kv/reb_in_100M.test'),
                    string(name: 'cluster', value: 'triton_kv.spec'),
                    string(name: 'version', value: params.version)
                ]
            }
        }

        stage('N1QL') {
            steps {
                build job: 'triton', propagate: false, parameters: [
                    string(name: 'test_config', value: 'n1ql/n1ql_thr_Q1_5M.test'),
                    string(name: 'cluster', value: 'triton.spec'),
                    string(name: 'version', value: params.version)
                ]
                build job: 'triton', propagate: false, parameters: [
                    string(name: 'test_config', value: 'n1ql/n1ql_thr_Q2_5M_moi_false.test'),
                    string(name: 'cluster', value: 'triton.spec'),
                    string(name: 'version', value: params.version)
                ]
                build job: 'triton', propagate: false, parameters: [
                    string(name: 'test_config', value: 'n1ql/n1ql_thr_Q2_5M_moi_ok.test'),
                    string(name: 'cluster', value: 'triton.spec'),
                    string(name: 'version', value: params.version)
                ]
                build job: 'triton', propagate: false, parameters: [
                    string(name: 'test_config', value: 'n1ql/n1ql_thr_Q2_5M_plasma_false.test'),
                    string(name: 'cluster', value: 'triton.spec'),
                    string(name: 'version', value: params.version)
                ]
                build job: 'triton', propagate: false, parameters: [
                    string(name: 'test_config', value: 'n1ql/n1ql_thr_Q2_5M_plasma_ok.test'),
                    string(name: 'cluster', value: 'triton.spec'),
                    string(name: 'version', value: params.version)
                ]
            }
        }

        stage('Tools') {
            steps {
                build job: 'triton', propagate: false, parameters: [
                    string(name: 'test_config', value: 'tools/backup_100M.test'),
                    string(name: 'cluster', value: 'triton_kv.spec'),
                    string(name: 'version', value: params.version)
                ]
            }
        }

        stage('XDCR') {
            steps {
                build job: 'triton', propagate: false, parameters: [
                    string(name: 'test_config', value: 'xdcr/xdcr_init_1x1_unidir_25M.test'),
                    string(name: 'cluster', value: 'triton_2x2.spec'),
                    string(name: 'version', value: params.version)
                ]
                build job: 'triton', propagate: false, parameters: [
                    string(name: 'test_config', value: 'xdcr/xdcr_init_1x1_unidir_25M_opt.test'),
                    string(name: 'cluster', value: 'triton_2x2.spec'),
                    string(name: 'version', value: params.version)
                ]
            }
        }

        stage('YCSB') {
            steps {
                build job: 'triton', propagate: false, parameters: [
                    string(name: 'test_config', value: 'ycsb/ycsb_workloada_10M.test'),
                    string(name: 'cluster', value: 'triton_ycsb.spec'),
                    string(name: 'version', value: params.version)
                ]
                build job: 'triton', propagate: false, parameters: [
                    string(name: 'test_config', value: 'ycsb/ycsb_workloada_10M_ephemeral.test'),
                    string(name: 'cluster', value: 'triton_ycsb.spec'),
                    string(name: 'version', value: params.version)
                ]
                build job: 'triton', propagate: false, parameters: [
                    string(name: 'test_config', value: 'ycsb/ycsb_workloadd_100M.test'),
                    string(name: 'cluster', value: 'triton_kv.spec'),
                    string(name: 'version', value: params.version)
                ]
                build job: 'triton', propagate: false, parameters: [
                    string(name: 'test_config', value: 'ycsb/ycsb_workloade_10M_moi.test'),
                    string(name: 'cluster', value: 'triton_ycsb.spec'),
                    string(name: 'version', value: params.version)
                ]
            }
        }

        stage('Notifications') {
            steps {
                build job: 'triton-notification', propagate: false, parameters: [
                    string(name: 'version', value: params.version)
                ]
            }
        }
    }
}
