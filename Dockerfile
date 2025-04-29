spring:
    application:
      name: Health
  
    r2dbc:
      url: r2dbc:postgresql://ep-long-band-a88ih3nz-pooler.eastus2.azure.neon.tech/neondb?sslmode=require
      username: neondb_owner
      password: npg_3QfxSB8phuOH
  
  server:
    port: 8085
  