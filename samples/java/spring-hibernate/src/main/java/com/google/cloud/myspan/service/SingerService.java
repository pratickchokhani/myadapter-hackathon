/**
 *
 */
package com.google.cloud.myspan.service;

import com.google.cloud.myspan.dao.SingersDao;
import com.google.cloud.myspan.entity.Albums;
import com.google.cloud.myspan.entity.Concerts;
import com.google.cloud.myspan.entity.MultiEntryIds;
import com.google.cloud.myspan.entity.Singers;
import com.google.cloud.myspan.entity.Tracks;
import com.google.cloud.myspan.entity.TracksId;
import com.google.cloud.myspan.entity.Utils;
import com.google.cloud.myspan.entity.Venues;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 *
 * @author Gulam Mustafa
 */
@Service
public class SingerService {

  @Autowired
  private SingersDao singersDao;

  public List<Singers> getSingers() {
    return singersDao.getAllSingers();
  }

  public Concerts getConcerts(UUID uuid) {
    return singersDao.getConcert(uuid);
  }

  public Albums getAlbum(UUID albumId) {
    return singersDao.getAlbum(albumId);
  }

  public Singers getSingers(UUID singers) {
    return singersDao.getSingers(singers);
  }

  public Albums updateAlbumMarketingBudget(UUID albumId, String marketingBudget) {
    Albums albums = singersDao.getAlbum(albumId);
    albums.setMarketingBudget(new BigDecimal(marketingBudget));
    singersDao.updateAlbumMarketingBudget(albums);
    return albums;
  }

  public List<Tracks> getTracksForAlbums(UUID albumId) {
    return singersDao.getTracks(albumId);
  }

  public UUID addSinger(String firstName, String lastName) {
    return singersDao.insertSinger(new Singers(firstName, lastName));
  }

  public UUID rollback() {
    return singersDao.rollbackTest(new Singers("Pratick", "Chokhani"));
  }

  public MultiEntryIds addTracksWithSingers(String trackName, int trackNumber, int sampleRate,
      String concertName, String venueName, String venueDescription, String albumName,
      String marketingBudget, String singerFirstName, String singerLastName) {

    final Singers singers = Utils.createSingers(singerFirstName, singerLastName);
    final Albums albums = Utils.createAlbums(singers, albumName, marketingBudget);
    final Venues venues = Utils.createVenue(venueName, venueDescription);
    final Concerts concerts1 = Utils.createConcerts(singers, venues, concertName);
    final Tracks tracks1 = Utils.createTracks(albums.getId(), trackName, trackNumber, sampleRate);
    singersDao.multiObjectsInTransaction(singers, albums, venues, concerts1, tracks1);
    return new MultiEntryIds(tracks1.getId(), albums.getId(), concerts1.getId(), singers.getId(),
        venues.getId());
  }


}
