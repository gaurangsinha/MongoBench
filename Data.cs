using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using FileHelpers;

using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Driver.Builders;
using System.Configuration;
using System.Diagnostics;

namespace MongoBench {
    /// <summary>
    /// 
    /// </summary>
    public class Data {
        
        /// <summary>
        /// Post documents
        /// </summary>
        private static BsonDocument[] POSTS;

        /// <summary>
        /// Individual comments
        /// </summary>
        private static BsonDocument[] COMMENTS;
        
        /// <summary>
        /// Constructor
        /// </summary>
        public static bool Initialize() {
            var commentEngine = new FileHelperEngine(typeof(Comment));
            commentEngine.Options.IgnoreFirstLines = 1;
            commentEngine.Options.IgnoreEmptyLines = true;
            commentEngine.ErrorManager.ErrorMode = ErrorMode.IgnoreAndContinue;
            Comment[] commentData = null;
            try {
                commentData = commentEngine.ReadFile("comments.csv") as Comment[];
            }
            catch (Exception ex) {
                Debug.Write(ex.ToString());
                return false;
            }
            COMMENTS = Array.ConvertAll<Comment, BsonDocument>(commentData,
                            c => new BsonDocument() {
                                { "author", c.author },
                                { "text", c.text },
                                { "date", c.date }
                            });
            
            var postEngine = new FileHelperEngine(typeof(Post));
            postEngine.Options.IgnoreFirstLines = 1;
            postEngine.Options.IgnoreEmptyLines = true;
            postEngine.ErrorManager.ErrorMode = ErrorMode.IgnoreAndContinue;
            Post[] postData = null;
            try {
                postData = postEngine.ReadFile("posts.csv") as Post[];
            }
            catch (Exception ex) {
                Debug.Write(ex.ToString());
                return false;
            }
            var rnd = new Random(DateTime.Now.Second);
            var minCommentCount = int.Parse(ConfigurationManager.AppSettings["MIN_COMMENT_COUNT"]);
            var maxCommentCount = int.Parse(ConfigurationManager.AppSettings["MAX_COMMENT_COUNT"]);
            POSTS = Array.ConvertAll<Post, BsonDocument>(postData,
                            p => new BsonDocument() { 
                                { "author", p.author },
                                { "title", p.title },
                                { "text", p.text },
                                { "tags", new BsonArray(p.tags.Split(' ')) },
                                { "comments", new BsonArray(COMMENTS.Take(rnd.Next(minCommentCount, maxCommentCount))) },
                                { "date", p.date }
                            });
            return true;
        }

        /// <summary>
        /// Gets the document.
        /// </summary>
        /// <param name="index">The index.</param>
        public static BsonDocument GetDocument(int index) {
            while(index >= POSTS.Length) 
                index -= POSTS.Length;
            return POSTS[index].DeepClone() as BsonDocument;
        }
    }

    [DelimitedRecord(",")]
    public class Post {
        [FieldQuoted('"', QuoteMode.OptionalForBoth)]
        public string author;
        
        [FieldQuoted('"', QuoteMode.OptionalForBoth)]
        public string title;
        
        [FieldQuoted('"', QuoteMode.OptionalForBoth)]
        public string text;
        
        [FieldQuoted('"', QuoteMode.OptionalForBoth)]
        public string tags;

        public int date;
    }

    [DelimitedRecord(",")]
    public class Comment {
        [FieldQuoted('"', QuoteMode.OptionalForBoth)]
        public string author;

        [FieldQuoted('"', QuoteMode.OptionalForBoth)]
        public string text;
        
        public int date;
    }
}
