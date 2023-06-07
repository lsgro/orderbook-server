use rust_decimal::prelude::*;
use std::collections::HashMap;

use crate::core::*;


#[derive(PartialEq, Debug)]
struct ConsolidateLevel {
    price: Decimal,
    exchange_levels: HashMap<String, Level>,
}

impl ConsolidateLevel {
    fn from_level(level: Level) -> ConsolidateLevel {
        ConsolidateLevel {
            price: level.price,
            exchange_levels: HashMap::from([(level.exchange.to_string(), level)])
        }
    }

    fn update(&mut self, level: Level) {
        assert_eq!(self.price, level.price);
        self.exchange_levels.insert(level.price.to_string(), level);
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

    fn update_side(side: &mut Vec<ConsolidateLevel>, side_update: Vec<Level>, low_price_first: bool) {
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
    fn test_empty_book() {
        let mut book = Consolidate::new();
        let book_update = BookUpdate {
            exchange: "test",
            bids: vec![
                Level::from_strs("test", "99", "10"),
                Level::from_strs("test", "98", "10"),
                Level::from_strs("test", "97", "10"),
            ],
            asks: vec![
                Level::from_strs("test", "100", "10"),
                Level::from_strs("test", "101", "10"),
                Level::from_strs("test", "102", "10"),
            ],
        };
        book.update(book_update);
        let exp_book = Consolidate {
            bids: vec![
                ConsolidateLevel::from_level(Level::from_strs("test", "99", "10")),
                ConsolidateLevel::from_level(Level::from_strs("test", "98", "10")),
                ConsolidateLevel::from_level(Level::from_strs("test", "97", "10")),
            ],
            asks: vec![
                ConsolidateLevel::from_level(Level::from_strs("test", "100", "10")),
                ConsolidateLevel::from_level(Level::from_strs("test", "101", "10")),
                ConsolidateLevel::from_level(Level::from_strs("test", "102", "10")),
            ]
        };
        assert_eq!(book, exp_book);
    }

    #[test]
    fn test_insert_into_bids_side() {
        let mut bids = vec![
            ConsolidateLevel::from_level(Level::from_strs("test1", "99", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test1", "97", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test1", "95", "10")),
        ];
        let bids_update = vec![
            Level::from_strs("test2", "100", "10"),
            Level::from_strs("test2", "98", "10"),
            Level::from_strs("test2", "94", "10"),
        ];
        Consolidate::update_side(&mut bids, bids_update, false);
        let exp_bids = vec![
            ConsolidateLevel::from_level(Level::from_strs("test2", "100", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test1", "99", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test2", "98", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test1", "97", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test1", "95", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test2", "94", "10")),
        ];
        assert_eq!(bids, exp_bids);
    }

    #[test]
    fn test_add_at_beginning_to_bids_side() {
        let mut bids = vec![
            ConsolidateLevel::from_level(Level::from_strs("test1", "99", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test1", "97", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test1", "95", "10")),
        ];
        let bids_update = vec![
            Level::from_strs("test2", "102", "10"),
            Level::from_strs("test2", "101", "10"),
            Level::from_strs("test2", "100", "10"),
        ];
        Consolidate::update_side(&mut bids, bids_update, false);
        let exp_bids = vec![
            ConsolidateLevel::from_level(Level::from_strs("test2", "102", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test2", "101", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test2", "100", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test1", "99", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test1", "97", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test1", "95", "10")),
        ];
        assert_eq!(bids, exp_bids);
    }

    #[test]
    fn test_add_at_end_to_bids_side() {
        let mut bids = vec![
            ConsolidateLevel::from_level(Level::from_strs("test1", "99", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test1", "97", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test1", "95", "10")),
        ];
        let bids_update = vec![
            Level::from_strs("test2", "94", "10"),
            Level::from_strs("test2", "93", "10"),
            Level::from_strs("test2", "92", "10"),
        ];
        Consolidate::update_side(&mut bids, bids_update, false);
        let exp_bids = vec![
            ConsolidateLevel::from_level(Level::from_strs("test1", "99", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test1", "97", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test1", "95", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test2", "94", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test2", "93", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test2", "92", "10")),
        ];
        assert_eq!(bids, exp_bids);
    }

    #[test]
    fn test_insert_into_asks_side() {
        let mut asks = vec![
            ConsolidateLevel::from_level(Level::from_strs("test1", "102", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test1", "104", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test1", "106", "10")),        ];
        let asks_update = vec![
            Level::from_strs("test2", "103", "10"),
            Level::from_strs("test2", "105", "10"),
            Level::from_strs("test2", "107", "10"),
        ];
        Consolidate::update_side(&mut asks, asks_update, true);
        let exp_asks = vec![
            ConsolidateLevel::from_level(Level::from_strs("test1", "102", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test2", "103", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test1", "104", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test2", "105", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test1", "106", "10")),
            ConsolidateLevel::from_level(Level::from_strs("test2", "107", "10")),
        ];
        assert_eq!(asks, exp_asks);
    }

    #[test]
    fn test_insert_into_book() {
        let mut book = Consolidate {
            bids: vec![
                ConsolidateLevel::from_level(Level::from_strs("test1", "99", "10")),
                ConsolidateLevel::from_level(Level::from_strs("test1", "97", "10")),
                ConsolidateLevel::from_level(Level::from_strs("test1", "95", "10")),
            ],
            asks: vec![
                ConsolidateLevel::from_level(Level::from_strs("test1", "102", "10")),
                ConsolidateLevel::from_level(Level::from_strs("test1", "104", "10")),
                ConsolidateLevel::from_level(Level::from_strs("test1", "106", "10")),
            ]
        };
        let book_update = BookUpdate {
            exchange: "test2",
            bids: vec![
                Level::from_strs("test2", "100", "10"),
                Level::from_strs("test2", "98", "10"),
                Level::from_strs("test2", "94", "10"),
            ],
            asks: vec![
                Level::from_strs("test2", "103", "10"),
                Level::from_strs("test2", "105", "10"),
                Level::from_strs("test2", "107", "10"),
            ],
        };
        let exp_book = Consolidate {
            bids: vec![
                ConsolidateLevel::from_level(Level::from_strs("test2", "100", "10")),
                ConsolidateLevel::from_level(Level::from_strs("test1", "99", "10")),
                ConsolidateLevel::from_level(Level::from_strs("test2", "98", "10")),
                ConsolidateLevel::from_level(Level::from_strs("test1", "97", "10")),
                ConsolidateLevel::from_level(Level::from_strs("test1", "95", "10")),
                ConsolidateLevel::from_level(Level::from_strs("test2", "94", "10")),
            ],
            asks: vec![
                ConsolidateLevel::from_level(Level::from_strs("test1", "102", "10")),
                ConsolidateLevel::from_level(Level::from_strs("test2", "103", "10")),
                ConsolidateLevel::from_level(Level::from_strs("test1", "104", "10")),
                ConsolidateLevel::from_level(Level::from_strs("test2", "105", "10")),
                ConsolidateLevel::from_level(Level::from_strs("test1", "106", "10")),
                ConsolidateLevel::from_level(Level::from_strs("test2", "107", "10")),
            ]
        };
        book.update(book_update);
        assert_eq!(book, exp_book);
    }
}