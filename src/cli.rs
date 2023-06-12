//! Utility to parse command line arguments for server and client programs.

use std::env::Args;
use crate::core::CurrencyPair;


const DEFAULT_PORT: u16 = 50000;
const DEFAULT_MESSAGE_NUM: usize = 500;
const CURRENCY_PAIR_MESSAGE: &str = "ERROR: argument <currency pair> must have shape cur1-cur2 (e.g. ETH-BTC)";


/// Utility class to help with command line option parsing.
pub struct ArgParser {
    args: Args,
    usage: &'static str,
}

impl ArgParser {
    pub fn new(mut args: Args, usage: &'static str) -> Self {
        let _ = args.next();
        Self { args, usage }
    }

    pub fn extract_currency_pair(&mut self) -> CurrencyPair {
        let pair_str = self.args.next().expect(self.usage);
        assert!(pair_str.len() >= 7 && pair_str.contains('-'), "{}", CURRENCY_PAIR_MESSAGE);
        let mut cur_strs = pair_str.split('-');
        let main = cur_strs.next().expect(CURRENCY_PAIR_MESSAGE).to_string();
        let counter = cur_strs.next().expect(CURRENCY_PAIR_MESSAGE).to_string();
        CurrencyPair { main, counter }
    }

    pub fn extract_message_num(&mut self) -> usize {
        let msg_num_str = self.args.next();
        let msg_num_res = msg_num_str.as_deref().map(|s| s.parse()).unwrap_or(Ok(DEFAULT_MESSAGE_NUM));
        match msg_num_res {
            Err(_) => panic!("Could not parse provided number {} as usize", msg_num_res.unwrap()),
            Ok(n) => n
        }
    }

    pub fn extract_port(&mut self) -> u16 {
        let port_str = self.args.next();
        let port_res = port_str.as_deref().map(|s| s.parse()).unwrap_or(Ok(DEFAULT_PORT));
        match port_res {
            Err(_) => panic!("Could not parse provided port number {} as u16", port_str.unwrap()),
            Ok(p) => p
        }
    }
}