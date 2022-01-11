// https://www.youtube.com/watch?v=HrOdDKOPqhg
//@ts-check
const https = require('https');

/**
 * @param {!Object} req Cloud Function request context = request from Fivetran.
 * @param {!Object} res Cloud Function response context = response back to Fivetran.
 */
exports.handler = (req, res) => {
    console.log('With access token from twitter, fetching tweets...')
    // Biztory's twitter user id
    var user_id = '1025820434'
    var path = `/2/users/${user_id}/tweets?expansions=author_id&tweet.fields=created_at&user.fields=created_at`
    // If this is not the first request, append since_id={since_id} to the request
    // This will get us only the tweets since last tweet in database
    if (req.body.state.since_id != null) {
        console.log(`...getting new tweets since id ${req.body.state.since_id}...`)
        path += '&since_id=' + req.body.state.since_id
    }
    // Get the recent tweets
    let get = https.get({
        hostname: 'api.twitter.com',
        path: path,
        headers: {
            'Authorization': 'Bearer ' + req.body.secrets.accessToken
        }
    }, getRes => {
        var reply = ''

        getRes.on('data', chunk => reply += chunk)
        getRes.on('end', () => withTweets(JSON.parse(reply)))
    })

    // Once we get the tweets timeline, we use it to upsert data into warehouse
    function withTweets(timeline) {
        console.log(`...got ${timeline.data.length} tweets, sending to Fivetran...`)
        // Keep track of the most recent id, so future updates are incremental
        let since_id = null
        // Reformat Twitter's response into nice, flat tables
        let tweets = []
        let users = []
        for (let t of timeline.data) {
            // // Remember the first id we encounter, which is the most recent
            if (since_id == null) {
                since_id = t.id
            }
            // Add all tweets
            tweets.push({
                id: t.id,
                user_id: t.author_id,
                created_at: t.created_at,
                text: t.text
            })
        }
        // Add user info
        // We don't have to worry about duplication - fivetran will take care of that
        for (let u of timeline.includes.users) {
            users.push({
                id: u.id,
                username: u.username,
                screen_name: u.name,
                created_at: u.created_at
            })
        }
        // Save the id of the most recent tweet from the meta object
        if (since_id == null) {
            since_id = timeline.meta.newest_id
        }   
    
        // Send JSON response back to Fivetran
        res.header("Content-Type", "application/json")
        res.status(200).send({
            // Remember the most recent id, so our requests are incremental
            state: {
                since_id: since_id == null ? req.state.since_id : since_id
            },
            // Fivetran will use these primary keys to perform 'merge' operation,
            // so even if we send the same row twice, we'll only get one copy in the warehouse
            schema: {
                tweets: {
                    primary_key: ['id']
                },
                users: {
                    primary_key: ['id']
                }
            },
            // Insert these rows into my warehouse
            insert: {
                tweets: tweets,
                users: users
            },
            // If this is true, Fivetran will immediately call this function back
            // for more data. This is useful to page through large collections.
            hasMore: false
        })
    }
}