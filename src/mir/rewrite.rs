use mir::MirQuery;
use nom_sql::Column;

pub fn pull_required_base_columns(q: &mut MirQuery) {
    let mut queue = Vec::new();
    queue.push(q.leaf.clone());

    while !queue.is_empty() {
        let mn = queue.pop().unwrap();

        let columns: Vec<Column> = mn.borrow()
            .columns()
            .into_iter()
            .cloned()
            .collect();

        let vn = mn.borrow().versioned_name();
        for ancestor in mn.borrow().ancestors() {
            if ancestor.borrow().ancestors().len() == 0 {
                // base, do nothing
                continue;
            }
            for c in &columns {
                if c.table.is_some() && c.function.is_none() &&
                   !ancestor.borrow().columns().contains(c) {
                    ancestor.borrow_mut().add_column(c.clone());
                }
            }
            queue.push(ancestor.clone());
        }
    }
}

pub fn push_all_base_columns(q: &mut MirQuery) {
    let mut queue = Vec::new();
    queue.extend(q.roots.clone());

    while !queue.is_empty() {
        let mn = queue.pop().unwrap();
        let columns: Vec<Column> = mn.borrow()
            .columns()
            .into_iter()
            .cloned()
            .collect();
        for child in mn.borrow().children() {
            // N.B. this terminates before reaching the actual leaf, since the last node of the
            // query (before the MIR `Leaf` node) already carries the query name. (`Leaf` nodes are
            // virtual nodes that will be removed and converted into materializations.)
            if child.borrow().versioned_name() == q.leaf.borrow().versioned_name() {
                continue;
            }
            for c in &columns {
                // push through if the child doesn't already have this column
                if !child.borrow().columns().contains(c) {
                    child.borrow_mut().add_column(c.clone());
                }
            }
            queue.push(child.clone());
        }
    }
}
