CREATE TABLE tblWatch (
	ticker_symbol VARCHAR ( 10 ) PRIMARY KEY,
	ticker_search_name VARCHAR ( 10 ),
	 ticker_close_price   INT,
	 max_tweet_id   INT,
	 cur_tweets   INT,
	 avg_tweets   INT,
	 cur_followers   INT,
	 avg_followers   INT,
	 cur_favorites   INT,
	 avg_favorites   INT,
	 cur_polarity   INT,
	 avg_polarity   INT,
	 cur_rockets   INT,
	 avg_rockets   INT,
	 cur_score   INT,
	 avg_score   INT,
	 cur_verified   INT,
	 avg_verified   INT
);

CREATE TABLE   tblHistory   (
	  tweet_id   VARCHAR,
	  created_at   INT,
	  ticker_symbol   VARCHAR,
	  screen_name   VARCHAR,
	  count   INT,
	  verified   INT,
	  replies   INT,
	  quotes   INT,
	  followers   INT,
	  favorites   INT,
	  polarity   INT,
	  rockets   INT,
	  score   INT
);

INSERT INTO tblWatch VALUES('XSPA','$XSPA');
INSERT INTO tblWatch VALUES('AMPE','$AMPE');
INSERT INTO tblWatch VALUES('NOVN','$NOVN');
INSERT INTO tblWatch VALUES('AIKI','$AIKI');
INSERT INTO tblWatch VALUES('ONTX','$ONTX');
INSERT INTO tblWatch VALUES('SNDL','$SNDL');
INSERT INTO tblWatch VALUES('BYM','$BYM');
INSERT INTO tblWatch VALUES('TWST','$TWST');
INSERT INTO tblWatch VALUES('SPCE','$SPCE');
INSERT INTO tblWatch VALUES('SPCE','$SPCE');
INSERT INTO tblWatch VALUES('ARKF','$ARKF');
INSERT INTO tblWatch VALUES('CRSR','$CRSR');