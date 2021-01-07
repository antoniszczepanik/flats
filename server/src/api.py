import re

from flask import Flask
from flask_restful import marshal_with, Resource, Api

from db import get_offers, get_offer, post_offer
from fields import OFFER_FIELDS
from parsers import post_parser


app = Flask(__name__)
api = Api(app)

class Offers(Resource):
    @marshal_with(OFFER_FIELDS)
    def get(self):
        return get_offers()

    @marshal_with(OFFER_FIELDS)
    def post(self):
        args = post_parser.parse_args()
        app.logger.info(args)
        app.logger.warning(args)
        return post_offer(args), 201

class Offer(Resource):
    @marshal_with(OFFER_FIELDS)
    def get(self, offer_id=None):
        if offer_id is None:
            return "Should include offer id", 404
        return get_offer(offer_id)


api.add_resource(Offers, '/offers/')
api.add_resource(Offer, '/offers/<string:offer_id>')

if __name__ == '__main__':
    app.config['DEBUG'] = True
    app.run(host='0.0.0.0', port="5000")
