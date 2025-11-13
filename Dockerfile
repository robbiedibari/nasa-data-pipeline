# Use official PostgreSQL image
FROM postgres:latest
# Set the enviroment variables
ENV POSTGRES_DB=nasa

EXPOSE 5432
