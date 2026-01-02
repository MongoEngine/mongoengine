// Ref:
// - https://www.mongodb.com/resources/products/compatibilities/deploying-a-mongodb-cluster-with-docker
// - https://www.mongodb.com/docs/manual/reference/method/rs.initiate/#mongodb-method-rs.initiate
try {
    rs.status();
} catch (e) {
    rs.initiate({
        _id: "mongoengine", members: [{_id: 0, host: "localhost:27017"}]
    });
}
