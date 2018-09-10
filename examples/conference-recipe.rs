extern crate distributary;

use distributary::ControllerBuilder;

use std::thread;
use std::time::Duration;

fn main() {
    // inline recipe definition
    let sql = "# base tables
               CREATE TABLE User (uid int, name varchar(255), primary key (uid));
               CREATE TABLE `Conflict` (principal varchar(255), `conflict` varchar(255));
               CREATE TABLE Paper (pid int, title text, author int);
               CREATE TABLE Review (rid int, content text, author int, pid int);

               # read queries
               PaperWithReview: SELECT Paper.pid AS pid, rid, content, title, \
                            Review.author AS rauth, Paper.author AS pauth \
                            FROM Paper, Review \
                            WHERE Paper.pid = Review.pid;
               PaperWithConf: SELECT * \
                            FROM PaperWithReview \
                            LEFT JOIN `Conflict` \
                            ON (PaperWithReview.pauth = `Conflict`.`conflict`);
               QUERY NotConflicted: \
                            SELECT * \
                            FROM PaperWithConf \
                            WHERE (PaperWithConf.principal IS NULL \
                            OR PaperWithConf.principal != 2);";

    // PaperReviewByUser: SELECT uid, pid, rid, content, title, Review.author, Paper.author \
    //                            FROM User, PaperWithReview;   
    let persistence_params = distributary::PersistenceParameters::new(
        distributary::DurabilityMode::DeleteOnExit,
        512,
        Duration::from_millis(1),
        Some(String::from("example")),
        1,
    );

    // set up Soup via recipe
    let mut builder = ControllerBuilder::default();
    builder.log_with(distributary::logger_pls());
    builder.set_persistence(persistence_params);

    let mut blender = builder.build_local().unwrap();
    blender.install_recipe(sql).unwrap();
    println!("{}", blender.graphviz().unwrap());

    // Get mutators and getter.
    let mut user = blender.table("User").unwrap();
    let mut conflict = blender.table("Conflict").unwrap();
    let mut paper = blender.table("Paper").unwrap();
    let mut review  = blender.table("Review").unwrap();
    let mut nc = blender.view("NotConflicted").unwrap();

    println!("Looking up user...");
    let uid = 1;
    // Make sure the user exists:
    // TODO: what if multiple columns have UID?
    if nc.lookup(&[uid.into()], true).unwrap().is_empty() {
        println!("Creating new users...");
        let uid1 = 1;
        let uid2 = 2;
        let uid3 = 3;
        let name1 = "harry potter";
        let name2 = "ron weasley";
        let name3 = "susan bones";
        user
            .insert(vec![uid1.into(), name1.into()])
            .unwrap();
        user
            .insert(vec![uid2.into(), name2.into()])
            .unwrap();
        user
            .insert(vec![uid3.into(), name3.into()])
            .unwrap();
    }

    // Add papers
    let pid1 = 1;
    let pid2 = 2;
    let title1 = "Using expelliarmus";
    let author1 = 1;
    let title2 = "Herbology";
    let author2 = 3;
    paper
        .insert(vec![pid1.into(), title1.into(), author1.into()])
        .unwrap();
    paper
        .insert(vec![pid2.into(), title2.into(), author2.into()])
        .unwrap();
    
    // Add reviews
    let rid1 = 1;
    let rid2 = 2;
    let content1 = "Great paper! Accept.";
    let content2 = "Needs work.";
    let author1 = 2;
    let author2 = 3;
    let pid1 = 2;
    let pid2 = 1;
    review
        .insert(vec![rid1.into(), content1.into(), author1.into(), pid1.into()])
        .unwrap();
    review
        .insert(vec![rid2.into(), content2.into(), author2.into(), pid2.into()])
        .unwrap();
    
    // Add conflicts
    let principal = 2;
    let conflict_id = 1;
    conflict
        .insert(vec![principal.into(), conflict_id.into()])
        .unwrap();

    // Wait for changes to propagate
    println!("Finished writing! Let's wait for things to propagate...");
    thread::sleep(Duration::from_millis(1000));

    // Read from table
    println!("Reading...");
    println!("{:#?}", nc.lookup(&[0.into()], true))
}
