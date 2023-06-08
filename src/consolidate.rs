use rust_decimal::prelude::*;
use std::collections::HashMap;

use crate::core::*;


#[derive(PartialEq, Debug)]
struct ConsolidateLevel {
    price: Decimal,
    exchange_levels: HashMap<&'static str, ExchangeLevel>,
}

impl ConsolidateLevel {
    fn from_level(level: ExchangeLevel) -> ConsolidateLevel {
        ConsolidateLevel {
            price: level.price,
            exchange_levels: HashMap::from([(level.exchange, level)]),
        }
    }

    fn from_levels(levels: Vec<ExchangeLevel>) -> ConsolidateLevel {
        assert!(!levels.is_empty());
        let mut levels_iter = levels.into_iter();
        let mut cons_level = ConsolidateLevel::from_level(levels_iter.next().unwrap());
        for level in levels_iter {
            cons_level.update(level);
        }
        cons_level
    }

    fn update(&mut self, level: ExchangeLevel) {
        assert_eq!(self.price, level.price);
        self.exchange_levels.insert(level.exchange, level);
    }

    fn total_amount(&self) -> Decimal {
        let mut result: Decimal = Decimal::zero();
        for level in self.exchange_levels.values() {
            result += level.amount;
        }
        result
    }
}

#[derive(PartialEq, Debug)]
struct Consolidate {
    bids: Vec<ConsolidateLevel>,
    asks: Vec<ConsolidateLevel>,
}

impl Consolidate {
    fn new() -> Consolidate {
        Consolidate {
            bids: vec![],
            asks: vec![],
        }
    }

    fn update(&mut self, book_update: BookUpdate) {
        Consolidate::update_side(&mut self.bids, book_update.bids, false);
        Consolidate::update_side(&mut self.asks, book_update.asks, true);
    }

    fn update_side(side: &mut Vec<ConsolidateLevel>, side_update: Vec<ExchangeLevel>, low_price_first: bool) {
        if side.is_empty() {
            side.extend(side_update.into_iter().map(ConsolidateLevel::from_level));
        } else {
            let mut current_index: usize = 0;
            let mut last_price: Decimal = side[side.len() - 1].price;
            for level_update in side_update {
                while current_index < NUM_LEVELS {
                    if side.len() >= NUM_LEVELS {
                        if low_price_first && level_update.price > last_price ||
                            !low_price_first && level_update.price < last_price {
                            return;
                        }
                    }
                    if current_index == side.len() && current_index < NUM_LEVELS {
                        last_price = level_update.price;
                        side.push(ConsolidateLevel::from_level(level_update));
                        current_index += 1;
                        break;
                    } else if low_price_first && level_update.price < side[current_index].price ||
                        !low_price_first && level_update.price > side[current_index].price {
                        side.insert(current_index, ConsolidateLevel::from_level(level_update));
                        current_index += 1;
                        break;
                    } else if level_update.price == side[current_index].price {
                        side[current_index].update(level_update);
                        current_index += 1;
                        break;
                    }
                    current_index += 1;
                    if current_index == NUM_LEVELS {
                        return;
                    }
                }
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consolidate_level_create_from_level() {
        let level = ExchangeLevel::from_strs("test", "100.0", "99.9");
        let cons_level = ConsolidateLevel::from_level(level);
        assert_eq!(cons_level.price, Decimal::from_str("100.0").unwrap());
        assert_eq!(cons_level.total_amount(), Decimal::from_str("99.9").unwrap());
    }

    #[test]
    fn test_consolidate_level_create_from_levels() {
        let level1 = ExchangeLevel::from_strs("test1", "100.0", "99.9");
        let level2 = ExchangeLevel::from_strs("test2", "100.0", "99.9");
        let cons_level = ConsolidateLevel::from_levels(vec![level1, level2]);
        assert_eq!(cons_level.price, Decimal::from_str("100.0").unwrap());
        assert_eq!(cons_level.total_amount(), Decimal::from_str("199.8").unwrap());
    }

    #[test]
    fn test_consolidate_level_create_from_levels_panics() {
        let level1 = ExchangeLevel::from_strs("test1", "100.0", "99.9");
        let level2 = ExchangeLevel::from_strs("test2", "99.0", "99.9");
        let result = std::panic::catch_unwind(|| ConsolidateLevel::from_levels(vec![level1, level2]));
        assert!(result.is_err());
    }

    #[test]
    fn test_consolidate_level_update_correct() {
        let level1 = ExchangeLevel::from_strs("test1", "100.0", "99.9");
        let level2 = ExchangeLevel::from_strs("test2", "100.0", "90.0");
        let mut cons_level = ConsolidateLevel::from_level(level1);
        cons_level.update(level2);
        assert_eq!(cons_level.price, Decimal::from_str("100.0").unwrap());
        assert_eq!(cons_level.total_amount(), Decimal::from_str("189.9").unwrap());
    }

    #[test]
    fn test_consolidate_level_update_panics() {
        let level1 = ExchangeLevel::from_strs("test", "100.0", "99.9");
        let level2 = ExchangeLevel::from_strs("test", "99.0", "90.0");
        let mut cons_level = ConsolidateLevel::from_level(level1);
        let result = std::panic::catch_unwind(move || cons_level.update(level2));
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_book() {
        let mut book = Consolidate::new();
        let book_update = BookUpdate {
            exchange: "test",
            bids: vec![
                ExchangeLevel::from_strs("test", "99", "10"),
                ExchangeLevel::from_strs("test", "98", "10"),
                ExchangeLevel::from_strs("test", "97", "10"),
            ],
            asks: vec![
                ExchangeLevel::from_strs("test", "100", "10"),
                ExchangeLevel::from_strs("test", "101", "10"),
                ExchangeLevel::from_strs("test", "102", "10"),
            ],
        };
        book.update(book_update);
        let exp_book = Consolidate {
            bids: vec![
                ConsolidateLevel::from_level(ExchangeLevel::from_strs("test", "99", "10")),
                ConsolidateLevel::from_level(ExchangeLevel::from_strs("test", "98", "10")),
                ConsolidateLevel::from_level(ExchangeLevel::from_strs("test", "97", "10")),
            ],
            asks: vec![
                ConsolidateLevel::from_level(ExchangeLevel::from_strs("test", "100", "10")),
                ConsolidateLevel::from_level(ExchangeLevel::from_strs("test", "101", "10")),
                ConsolidateLevel::from_level(ExchangeLevel::from_strs("test", "102", "10")),
            ]
        };
        assert_eq!(book, exp_book);
    }

    #[test]
    fn test_insert_into_bids_side() {
        let mut bids = vec![
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "99", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "97", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "95", "10")),
        ];
        let bids_update = vec![
            ExchangeLevel::from_strs("test2", "100", "10"),
            ExchangeLevel::from_strs("test2", "98", "10"),
            ExchangeLevel::from_strs("test2", "94", "10"),
        ];
        Consolidate::update_side(&mut bids, bids_update, false);
        let exp_bids = vec![
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test2", "100", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "99", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test2", "98", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "97", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "95", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test2", "94", "10")),
        ];
        assert_eq!(bids, exp_bids);
    }

    #[test]
    fn test_add_at_beginning_to_bids_side() {
        let mut bids = vec![
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "99", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "97", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "95", "10")),
        ];
        let bids_update = vec![
            ExchangeLevel::from_strs("test2", "102", "10"),
            ExchangeLevel::from_strs("test2", "101", "10"),
            ExchangeLevel::from_strs("test2", "100", "10"),
        ];
        Consolidate::update_side(&mut bids, bids_update, false);
        let exp_bids = vec![
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test2", "102", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test2", "101", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test2", "100", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "99", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "97", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "95", "10")),
        ];
        assert_eq!(bids, exp_bids);
    }

    #[test]
    fn test_add_at_end_to_bids_side() {
        let mut bids = vec![
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "99", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "97", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "95", "10")),
        ];
        let bids_update = vec![
            ExchangeLevel::from_strs("test2", "94", "10"),
            ExchangeLevel::from_strs("test2", "93", "10"),
            ExchangeLevel::from_strs("test2", "92", "10"),
        ];
        Consolidate::update_side(&mut bids, bids_update, false);
        let exp_bids = vec![
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "99", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "97", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "95", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test2", "94", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test2", "93", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test2", "92", "10")),
        ];
        assert_eq!(bids, exp_bids);
    }

    #[test]
    fn test_insert_into_asks_side() {
        let mut asks = vec![
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "102", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "104", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "106", "10")),
        ];
        let asks_update = vec![
            ExchangeLevel::from_strs("test2", "103", "10"),
            ExchangeLevel::from_strs("test2", "105", "10"),
            ExchangeLevel::from_strs("test2", "107", "10"),
        ];
        Consolidate::update_side(&mut asks, asks_update, true);
        let exp_asks = vec![
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "102", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test2", "103", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "104", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test2", "105", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "106", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test2", "107", "10")),
        ];
        assert_eq!(asks, exp_asks);
    }

    #[test]
    fn test_add_at_beginning_to_asks_side() {
        let mut asks = vec![
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "102", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "104", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "106", "10")),
        ];
        let asks_update = vec![
            ExchangeLevel::from_strs("test2", "99", "10"),
            ExchangeLevel::from_strs("test2", "100", "10"),
            ExchangeLevel::from_strs("test2", "101", "10"),
        ];
        Consolidate::update_side(&mut asks, asks_update, true);
        let exp_asks = vec![
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test2", "99", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test2", "100", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test2", "101", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "102", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "104", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "106", "10")),
        ];
        assert_eq!(asks, exp_asks);
    }

    #[test]
    fn test_add_at_end_to_asks_side() {
        let mut asks = vec![
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "102", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "104", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "106", "10")),
        ];
        let asks_update = vec![
            ExchangeLevel::from_strs("test2", "107", "10"),
            ExchangeLevel::from_strs("test2", "108", "10"),
            ExchangeLevel::from_strs("test2", "109", "10"),
        ];
        Consolidate::update_side(&mut asks, asks_update, true);
        let exp_asks = vec![
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "102", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "104", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "106", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test2", "107", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test2", "108", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test2", "109", "10")),
        ];
        assert_eq!(asks, exp_asks);
    }

    #[test]
    fn test_update_and_add_into_bids_side() {
        let mut bids = vec![
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "99", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "98", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "97", "10")),
        ];
        let bids_update = vec![
            ExchangeLevel::from_strs("test1", "99", "5"),
            ExchangeLevel::from_strs("test1", "98", "15"),
            ExchangeLevel::from_strs("test2", "96", "10"),
        ];
        Consolidate::update_side(&mut bids, bids_update, false);
        let level1 = &bids[0];
        assert_eq!(level1.price, Decimal::from_str("99").unwrap());
        assert_eq!(level1.total_amount(), Decimal::from_str("5").unwrap());
        let level2 = &bids[1];
        assert_eq!(level2.price, Decimal::from_str("98").unwrap());
        assert_eq!(level2.total_amount(), Decimal::from_str("15").unwrap());
        let level3 = &bids[2];
        assert_eq!(level3.price, Decimal::from_str("97").unwrap());
        assert_eq!(level3.total_amount(), Decimal::from_str("10").unwrap());
        let level4 = &bids[3];
        assert_eq!(level4.price, Decimal::from_str("96").unwrap());
        assert_eq!(level4.total_amount(), Decimal::from_str("10").unwrap());
    }

    #[test]
    fn test_merge_update_and_add_into_bids_side() {
        let mut bids = vec![
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "99", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "98", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "97", "10")),
        ];
        let bids_update = vec![
            ExchangeLevel::from_strs("test2", "99", "5"),
            ExchangeLevel::from_strs("test1", "98", "15"),
            ExchangeLevel::from_strs("test2", "96", "10"),
        ];
        Consolidate::update_side(&mut bids, bids_update, false);
        let level1 = &bids[0];
        assert_eq!(level1.price, Decimal::from_str("99").unwrap());
        assert_eq!(level1.total_amount(), Decimal::from_str("15").unwrap());
        let level2 = &bids[1];
        assert_eq!(level2.price, Decimal::from_str("98").unwrap());
        assert_eq!(level2.total_amount(), Decimal::from_str("15").unwrap());
        let level3 = &bids[2];
        assert_eq!(level3.price, Decimal::from_str("97").unwrap());
        assert_eq!(level3.total_amount(), Decimal::from_str("10").unwrap());
        let level4 = &bids[3];
        assert_eq!(level4.price, Decimal::from_str("96").unwrap());
        assert_eq!(level4.total_amount(), Decimal::from_str("10").unwrap());
    }
    
    #[test]
    fn test_merge_and_add_into_bids_side() {
        let mut bids = vec![
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "99", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "98", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "97", "10")),
        ];
        let bids_update = vec![
            ExchangeLevel::from_strs("test2", "99", "10"),
            ExchangeLevel::from_strs("test2", "98", "10"),
            ExchangeLevel::from_strs("test2", "96", "10"),
        ];
        Consolidate::update_side(&mut bids, bids_update, false);
        let level1 = &bids[0];
        assert_eq!(level1.price, Decimal::from_str("99").unwrap());
        assert_eq!(level1.total_amount(), Decimal::from_str("20").unwrap());
        let level2 = &bids[1];
        assert_eq!(level2.price, Decimal::from_str("98").unwrap());
        assert_eq!(level2.total_amount(), Decimal::from_str("20").unwrap());
        let level3 = &bids[2];
        assert_eq!(level3.price, Decimal::from_str("97").unwrap());
        assert_eq!(level3.total_amount(), Decimal::from_str("10").unwrap());
        let level4 = &bids[3];
        assert_eq!(level4.price, Decimal::from_str("96").unwrap());
        assert_eq!(level4.total_amount(), Decimal::from_str("10").unwrap());
    }

    #[test]
    fn test_merge_and_add_into_asks_side() {
        let mut asks = vec![
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "102", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "104", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "106", "10")),
        ];
        let asks_update = vec![
            ExchangeLevel::from_strs("test2", "102", "10"),
            ExchangeLevel::from_strs("test2", "104", "10"),
            ExchangeLevel::from_strs("test2", "109", "10"),
        ];
        Consolidate::update_side(&mut asks, asks_update, true);
        let level1 = &asks[0];
        assert_eq!(level1.price, Decimal::from_str("102").unwrap());
        assert_eq!(level1.total_amount(), Decimal::from_str("20").unwrap());
        let level2 = &asks[1];
        assert_eq!(level2.price, Decimal::from_str("104").unwrap());
        assert_eq!(level2.total_amount(), Decimal::from_str("20").unwrap());
        let level3 = &asks[2];
        assert_eq!(level3.price, Decimal::from_str("106").unwrap());
        assert_eq!(level3.total_amount(), Decimal::from_str("10").unwrap());
        let level4 = &asks[3];
        assert_eq!(level4.price, Decimal::from_str("109").unwrap());
        assert_eq!(level4.total_amount(), Decimal::from_str("10").unwrap());
    }

    #[test]
    fn test_update_and_add_into_asks_side() {
        let mut asks = vec![
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "102", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "104", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "106", "10")),
        ];
        let asks_update = vec![
            ExchangeLevel::from_strs("test1", "102", "5"),
            ExchangeLevel::from_strs("test1", "104", "15"),
            ExchangeLevel::from_strs("test2", "109", "10"),
        ];
        Consolidate::update_side(&mut asks, asks_update, true);
        let level1 = &asks[0];
        assert_eq!(level1.price, Decimal::from_str("102").unwrap());
        assert_eq!(level1.total_amount(), Decimal::from_str("5").unwrap());
        let level2 = &asks[1];
        assert_eq!(level2.price, Decimal::from_str("104").unwrap());
        assert_eq!(level2.total_amount(), Decimal::from_str("15").unwrap());
        let level3 = &asks[2];
        assert_eq!(level3.price, Decimal::from_str("106").unwrap());
        assert_eq!(level3.total_amount(), Decimal::from_str("10").unwrap());
        let level4 = &asks[3];
        assert_eq!(level4.price, Decimal::from_str("109").unwrap());
        assert_eq!(level4.total_amount(), Decimal::from_str("10").unwrap());
    }

    #[test]
    fn test_merge_update_and_add_into_asks_side() {
        let mut asks = vec![
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "102", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "104", "10")),
            ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "106", "10")),
        ];
        let asks_update = vec![
            ExchangeLevel::from_strs("test2", "102", "5"),
            ExchangeLevel::from_strs("test1", "104", "15"),
            ExchangeLevel::from_strs("test2", "109", "10"),
        ];
        Consolidate::update_side(&mut asks, asks_update, true);
        let level1 = &asks[0];
        assert_eq!(level1.price, Decimal::from_str("102").unwrap());
        assert_eq!(level1.total_amount(), Decimal::from_str("15").unwrap());
        let level2 = &asks[1];
        assert_eq!(level2.price, Decimal::from_str("104").unwrap());
        assert_eq!(level2.total_amount(), Decimal::from_str("15").unwrap());
        let level3 = &asks[2];
        assert_eq!(level3.price, Decimal::from_str("106").unwrap());
        assert_eq!(level3.total_amount(), Decimal::from_str("10").unwrap());
        let level4 = &asks[3];
        assert_eq!(level4.price, Decimal::from_str("109").unwrap());
        assert_eq!(level4.total_amount(), Decimal::from_str("10").unwrap());
    }

    #[test]
    fn test_book_amounts() {
        let mut book = Consolidate {
            bids: vec![
                ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "99", "10")),
                ConsolidateLevel::from_level(ExchangeLevel::from_strs("test2", "97", "10")),
                ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "95", "10")),
            ],
            asks: vec![
                ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "102", "10")),
                ConsolidateLevel::from_level(ExchangeLevel::from_strs("test1", "104", "10")),
                ConsolidateLevel::from_level(ExchangeLevel::from_strs("test2", "106", "10")),
            ]
        };
        let book_update1 = BookUpdate {
            exchange: "test1",
            bids: vec![
                ExchangeLevel::from_strs("test1", "100", "10"),
                ExchangeLevel::from_strs("test1", "97", "5"),
                ExchangeLevel::from_strs("test1", "95", "5"),
            ],
            asks: vec![
                ExchangeLevel::from_strs("test1", "103", "10"),
                ExchangeLevel::from_strs("test1", "105", "10"),
                ExchangeLevel::from_strs("test1", "106", "5"),
            ],
        };
        book.update(book_update1);
        let book_update2 = BookUpdate {
            exchange: "test2",
            bids: vec![
                ExchangeLevel::from_strs("test2", "100", "20"),
                ExchangeLevel::from_strs("test2", "97", "15"),
                ExchangeLevel::from_strs("test2", "94", "10"),
            ],
            asks: vec![
                ExchangeLevel::from_strs("test2", "102", "10"),
                ExchangeLevel::from_strs("test2", "105", "10"),
                ExchangeLevel::from_strs("test2", "107", "10"),
            ],
        };
        book.update(book_update2);

        assert_eq!(book.bids.len(), 5);
        let bid1 = &book.bids[0];
        assert_eq!(bid1.price, Decimal::from_str("100").unwrap());
        assert_eq!(bid1.total_amount(), Decimal::from_str("30").unwrap());
        let bid2 = &book.bids[1];
        assert_eq!(bid2.price, Decimal::from_str("99").unwrap());
        assert_eq!(bid2.total_amount(), Decimal::from_str("10").unwrap());
        let bid3 = &book.bids[2];
        assert_eq!(bid3.price, Decimal::from_str("97").unwrap());
        assert_eq!(bid3.total_amount(), Decimal::from_str("20").unwrap());
        let bid4 = &book.bids[3];
        assert_eq!(bid4.price, Decimal::from_str("95").unwrap());
        assert_eq!(bid4.total_amount(), Decimal::from_str("5").unwrap());
        let bid5 = &book.bids[4];
        assert_eq!(bid5.price, Decimal::from_str("94").unwrap());
        assert_eq!(bid5.total_amount(), Decimal::from_str("10").unwrap());

        assert_eq!(book.asks.len(), 6);
        let ask1 = &book.asks[0];
        assert_eq!(ask1.price, Decimal::from_str("102").unwrap());
        assert_eq!(ask1.total_amount(), Decimal::from_str("20").unwrap());
        let ask2 = &book.asks[1];
        assert_eq!(ask2.price, Decimal::from_str("103").unwrap());
        assert_eq!(ask2.total_amount(), Decimal::from_str("10").unwrap());
        let ask3 = &book.asks[2];
        assert_eq!(ask3.price, Decimal::from_str("104").unwrap());
        assert_eq!(ask3.total_amount(), Decimal::from_str("10").unwrap());
        let ask4 = &book.asks[3];
        assert_eq!(ask4.price, Decimal::from_str("105").unwrap());
        assert_eq!(ask4.total_amount(), Decimal::from_str("20").unwrap());
        let ask5 = &book.asks[4];
        assert_eq!(ask5.price, Decimal::from_str("106").unwrap());
        assert_eq!(ask5.total_amount(), Decimal::from_str("15").unwrap());
        let ask6 = &book.asks[5];
        assert_eq!(ask6.price, Decimal::from_str("107").unwrap());
        assert_eq!(ask6.total_amount(), Decimal::from_str("10").unwrap());
    }
}