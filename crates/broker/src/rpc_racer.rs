// Simplified RPC management module

pub struct RpcRacer {
    rpcs: Vec<String>,
}

impl RpcRacer {
    pub fn new() -> Self {
        Self {
            rpcs: vec![
                // 7개 RPC 모두 사용 (rpcs 파일 기준)
                "https://base-mainnet.g.alchemy.com/v2/LzOCLmaolNovcIhTvHpbNnBSnhGfSEFP".to_string(),
                "https://mainnet-preconf.base.org".to_string(), // Flashblocks - 200ms 블록
                "https://base-mainnet.infura.io/v3/b287021875ef4eba8d11bc98c32d78a4".to_string(),
                "https://cool-old-theorem.base-mainnet.quiknode.pro/90b7fb86615eee742b5c9bdcf46282072f690717/".to_string(),
                "https://base-mainnet.core.chainstack.com/7169d70533af0103ae9984799c80cf76".to_string(),
                "https://lb.drpc.org/base/ApklQs6JokXxjviH4hmf1eWmN3aZdDIR8IfXIgaNGuYu".to_string(),
                "https://api.zan.top/node/v1/base/mainnet/da21214226664fc9bb3136d61034c99c".to_string(),
            ],
        }
    }

    /// RPC 목록 반환 (단순화)
    pub fn get_rpcs(&self) -> &[String] {
        &self.rpcs
    }

    /// 단순화된 헬퍼 함수
    #[allow(dead_code)]
    pub fn get_count(&self) -> usize {
        self.rpcs.len()
    }
}