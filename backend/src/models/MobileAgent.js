const { randomUUID } = require('crypto');

class MobileAgent {
    constructor(task, originator, visited_brokers = [], results = []) {
        this.id = randomUUID();
        this.task = task;
        this.originator = originator;
        this.visited_brokers = visited_brokers;
        this.results = results;
    }

    toJSON() {
        return {
            id: this.id,
            task: this.task,
            originator: this.originator,
            visited_brokers: this.visited_brokers,
            results: this.results,
        };
    }
}

module.exports = MobileAgent;